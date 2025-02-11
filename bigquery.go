package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bluesky-social/indigo/api/bsky"
	_ "github.com/bluesky-social/indigo/api/chat"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/backfill"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
)

var plog = slog.Default()

// BigQueryBackend Handles interactions with BigQuery
type BigQueryBackend struct {
	s *Server

	bfstore *backfill.Gormstore

	client    *bigquery.Client
	dataset   *bigquery.Dataset
	projectID string
	datasetID string
	lastSeq   int64
	seqLk     sync.Mutex
}

type BQRecord struct {
	ID         string    `bigquery:"uri"`
	AuthorDID  string    `bigquery:"author_did"`
	Collection string    `bigquery:"collection"`
	Rkey       string    `bigquery:"rkey"`
	Created    time.Time `bigquery:"created_at"`
	Indexed    time.Time `bigquery:"indexed_at"`
	Raw        []byte    `bigquery:"raw"`
	Json       string    `bigquery:"json"`
	Cid        string    `bigquery:"cid"`
	Rev        string    `bigquery:"rev"`

	// post specific fields
	ReplyTo    bigquery.NullString `bigquery:"reply_to_uri"` // References another post ID
	ReplyToDID bigquery.NullString `bigquery:"reply_to_did"`
	InThread   bigquery.NullString `bigquery:"in_thread_uri"` // References root post ID
	Reposting  bigquery.NullString `bigquery:"reposting_uri"` // For quote posts
}

type BQInteraction struct {
	ID        string    `bigquery:"uri"` // Composite key: author_did/rkey
	Kind      string    `bigquery:"kind"`
	AuthorDID string    `bigquery:"author_did"`
	Rkey      string    `bigquery:"rkey"`
	Created   time.Time `bigquery:"created_at"`
	Indexed   time.Time `bigquery:"indexed_at"`
	SubjectID string    `bigquery:"subject_uri"` // References post ID
}

// BQFollow represents a follow record in BigQuery
type BQFollow struct {
	ID         string    `bigquery:"uri"`
	AuthorDID  string    `bigquery:"author_did"`
	Rkey       string    `bigquery:"rkey"`
	Created    time.Time `bigquery:"created_at"`
	Indexed    time.Time `bigquery:"indexed_at"`
	SubjectDID string    `bigquery:"subject_did"`
}

func recordUri(did, col, rkey string) string {
	return fmt.Sprintf("at://%s/%s/%s", did, col, rkey)
}

func postUri(did, rkey string) string {
	return fmt.Sprintf("at://%s/app.bsky.feed.post/%s", did, rkey)
}

func likeUri(did, rkey string) string {
	return fmt.Sprintf("at://%s/app.bsky.feed.like/%s", did, rkey)
}

func repostUri(did, rkey string) string {
	return fmt.Sprintf("at://%s/app.bsky.feed.repost/%s", did, rkey)
}

func followUri(did, rkey string) string {
	return fmt.Sprintf("at://%s/app.bsky.graph.follow/%s", did, rkey)
}

func (b *BigQueryBackend) HandleCreate(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error {
	start := time.Now()

	lrev, err := b.revForRepo(repo)
	if err != nil {
		return err
	}
	if lrev != "" {
		if rev < lrev {
			return nil
		}
	}

	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid path in HandleCreate: %q", path)
	}
	col := parts[0]
	rkey := parts[1]

	defer func() {
		handleOpHist.WithLabelValues("create", col).Observe(float64(time.Since(start).Milliseconds()))
	}()

	switch col {
	case "app.bsky.feed.post":
		if err := b.HandleCreatePost(ctx, repo, rkey, rev, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.like":
		if err := b.HandleCreateLike(ctx, repo, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.repost":
		if err := b.HandleCreateRepost(ctx, repo, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.follow":
		if err := b.HandleCreateFollow(ctx, repo, rkey, *rec, *cid); err != nil {
			return err
		}
	default:
		if err := b.HandleCreateGeneric(ctx, repo, col, rkey, rev, *rec, *cid); err != nil {
			return fmt.Errorf("handle generic record: %w", err)
		}
	}

	return nil
}

func (b *BigQueryBackend) HandleCreatePost(ctx context.Context, repo string, rkey string, rev string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedPost
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	jsonb, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	post := &BQRecord{
		ID:         postUri(repo, rkey),
		Collection: "app.bsky.feed.post",
		AuthorDID:  repo,
		Rkey:       rkey,
		Created:    created.Time(),
		Indexed:    time.Now(),
		Raw:        recb,
		Json:       string(jsonb),
		Cid:        cc.String(),
		Rev:        rev,
	}

	if rec.Reply != nil && rec.Reply.Parent != nil {
		if rec.Reply.Root == nil {
			return fmt.Errorf("post reply had nil root")
		}

		parentUri, err := syntax.ParseATURI(rec.Reply.Parent.Uri)
		if err != nil {
			return fmt.Errorf("parsing reply parent: %w", err)
		}

		post.ReplyTo = bigquery.NullString{
			StringVal: parentUri.String(),
			Valid:     true,
		}
		post.ReplyToDID = bigquery.NullString{
			StringVal: parentUri.Authority().String(),
			Valid:     true,
		}

		// Set thread reference
		rootUri := rec.Reply.Root.Uri
		post.InThread = bigquery.NullString{
			StringVal: rootUri,
			Valid:     true,
		}

	}

	if rec.Embed != nil {
		var quotedPostUri string
		if rec.Embed.EmbedRecord != nil && rec.Embed.EmbedRecord.Record != nil {
			quotedPostUri = rec.Embed.EmbedRecord.Record.Uri
		}
		if rec.Embed.EmbedRecordWithMedia != nil &&
			rec.Embed.EmbedRecordWithMedia.Record != nil &&
			rec.Embed.EmbedRecordWithMedia.Record.Record != nil {
			quotedPostUri = rec.Embed.EmbedRecordWithMedia.Record.Record.Uri
		}

		if quotedPostUri != "" && strings.Contains(quotedPostUri, "app.bsky.feed.post") {
			post.Reposting = bigquery.NullString{
				StringVal: quotedPostUri,
				Valid:     true,
			}
		}
	}

	inserter := b.dataset.Table("records").Inserter()
	if err := inserter.Put(ctx, post); err != nil {
		return fmt.Errorf("failed to insert post: %w", err)
	}

	return nil
}

func (b *BigQueryBackend) HandleCreateLike(ctx context.Context, repo string, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedLike
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := parseCreatedFromRecord(rec, rkey)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	subjectUri := rec.Subject.Uri

	like := &BQInteraction{
		ID:        likeUri(repo, rkey),
		Kind:      "like",
		AuthorDID: repo,
		Rkey:      rkey,
		Created:   created,
		Indexed:   time.Now(),
		SubjectID: subjectUri,
	}

	inserter := b.dataset.Table("interactions").Inserter()
	if err := inserter.Put(ctx, like); err != nil {
		return fmt.Errorf("failed to insert like: %w", err)
	}

	return nil
}

func (b *BigQueryBackend) HandleCreateRepost(ctx context.Context, repo string, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedRepost
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	subjectUri := rec.Subject.Uri

	repost := &BQInteraction{
		ID:        repostUri(repo, rkey),
		Kind:      "repost",
		AuthorDID: repo,
		Rkey:      rkey,
		Created:   created.Time(),
		Indexed:   time.Now(),
		SubjectID: subjectUri,
	}

	inserter := b.dataset.Table("interactions").Inserter()
	if err := inserter.Put(ctx, repost); err != nil {
		return fmt.Errorf("failed to insert repost: %w", err)
	}

	return nil
}

func (b *BigQueryBackend) HandleCreateFollow(ctx context.Context, repo string, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphFollow
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	follow := &BQFollow{
		ID:         followUri(repo, rkey),
		AuthorDID:  repo,
		Rkey:       rkey,
		Created:    created.Time(),
		Indexed:    time.Now(),
		SubjectDID: rec.Subject,
	}

	inserter := b.dataset.Table("follows").Inserter()
	if err := inserter.Put(ctx, follow); err != nil {
		return fmt.Errorf("failed to insert follow: %w", err)
	}

	return nil
}

func parseTimestamp(t string) (time.Time, error) {
	dt, err := syntax.ParseDatetimeLenient(t)
	if err != nil {
		return time.Time{}, err
	}

	return dt.Time(), nil
}

func inRange(t time.Time) bool {
	now := time.Now()
	if t.Before(now) {
		if now.Sub(t) > time.Hour*24*365*5 {
			return false
		}
		return true
	}

	if t.Sub(now) > time.Hour*24*200 {
		return false
	}

	return true
}

func parseCreatedFromRecord(rec any, rkey string) (time.Time, error) {
	var rkeyTime time.Time
	if rkey != "self" {
		rt, err := syntax.ParseTID(rkey)
		if err != nil {
			plog.Warn("failed to parse rkey for record into timestamp", "rkey", rkey, "error", err)
		} else {
			rkeyTime = rt.Time()
		}
	}

	switch rec := rec.(type) {
	case *bsky.FeedPost:
		pt, err := parseTimestamp(rec.CreatedAt)
		if err != nil {
			return time.Time{}, err
		}

		if inRange(pt) {
			return pt, nil
		}

		if rkeyTime.IsZero() {
			return time.Time{}, fmt.Errorf("failed to get a useful timestamp from record")
		}

		return rkeyTime, nil
	case *bsky.ActorProfile:
		if rec.CreatedAt == nil {
			// I hate this, but not many other good options
			return time.Now(), nil
		}

		return parseTimestamp(*rec.CreatedAt)
	case *bsky.FeedGenerator:
		if !rkeyTime.IsZero() && inRange(rkeyTime) {
			return rkeyTime, nil
		}
		return time.Now(), nil
	default:
		if rkeyTime.IsZero() {
			return time.Now(), nil
		}
		return rkeyTime, nil
	}
}

func (b *BigQueryBackend) HandleCreateGeneric(ctx context.Context, repo, collection, rkey string, rev string, recb []byte, cc cid.Cid) error {
	val, err := lexutil.CborDecodeValue(recb)
	if err != nil {
		return err
	}

	jsonb, err := json.Marshal(val)
	if err != nil {
		return err
	}

	created, err := parseCreatedFromRecord(val, rkey)
	if err != nil {
		return err
	}

	uri := recordUri(repo, collection, rkey)

	profile := &BQRecord{
		ID:         uri,
		Rkey:       rkey,
		AuthorDID:  repo,
		Collection: collection,
		Created:    created,
		Indexed:    time.Now(),
		Raw:        recb,
		Json:       string(jsonb),
		Rev:        rev,
		Cid:        cc.String(),
	}

	inserter := b.dataset.Table("records").Inserter()
	if err := inserter.Put(ctx, profile); err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
	}

	return nil
}

// Delete Handlers

func (b *BigQueryBackend) HandleDeleteRecord(ctx context.Context, repo, collection, rkey string) error {
	postID := postUri(repo, rkey)

	// Delete the post itself using DML
	deleteQuery := fmt.Sprintf("DELETE FROM %s.records WHERE id = @post_id", b.datasetID)
	q := b.client.Query(deleteQuery)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "post_id", Value: postID},
	}

	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run delete query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for delete job: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("delete job failed: %w", status.Err())
	}

	return nil
}

func (b *BigQueryBackend) HandleDeleteInteraction(ctx context.Context, repo, col, rkey string) error {
	id := recordUri(repo, col, rkey)

	// Delete the like
	deleteQuery := fmt.Sprintf("DELETE FROM %s.interactions WHERE id = @rec_id", b.datasetID)
	q := b.client.Query(deleteQuery)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "rec_id", Value: id},
	}

	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run delete query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for delete job: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("delete job failed: %w", status.Err())
	}

	return nil
}

func (b *BigQueryBackend) HandleDeleteFollow(ctx context.Context, repo string, rkey string) error {
	followID := followUri(repo, rkey)

	deleteQuery := fmt.Sprintf("DELETE FROM %s.follows WHERE id = @follow_id", b.datasetID)
	q := b.client.Query(deleteQuery)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "follow_id", Value: followID},
	}

	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run delete query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for delete job: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("delete job failed: %w", status.Err())
	}

	return nil
}

func (b *BigQueryBackend) HandleDeleteGeneric(ctx context.Context, repo, collection, rkey string) error {
	uri := recordUri(repo, collection, rkey)
	deleteQuery := fmt.Sprintf("DELETE FROM %s.records WHERE id = @uri", b.datasetID)
	q := b.client.Query(deleteQuery)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "uri", Value: uri},
	}

	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run delete query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for delete job: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("delete job failed: %w", status.Err())
	}

	return nil
}

// Update Handlers
func (b *BigQueryBackend) HandleUpdateGeneric(ctx context.Context, repo, collection, rkey string, rev string, recb []byte, cc cid.Cid) error {
	uri := recordUri(repo, collection, rkey)

	val, err := lexutil.CborDecodeValue(recb)
	if err != nil {
		return err
	}

	jsonb, err := json.Marshal(val)
	if err != nil {
		return err
	}

	created, err := parseCreatedFromRecord(val, rkey)
	if err != nil {
		return err
	}

	profile := &BQRecord{
		ID:        uri,
		AuthorDID: repo,
		Created:   created,
		Indexed:   time.Now(),
		Raw:       recb,
		Rkey:      rkey,
		Rev:       rev,
		Json:      string(jsonb),
	}

	// In BigQuery, we'll just insert a new row for the profile update
	// The query layer can Handle getting the latest profile by DID
	inserter := b.dataset.Table("records").Inserter()
	if err := inserter.Put(ctx, profile); err != nil {
		return fmt.Errorf("failed to insert updated record: %w", err)
	}

	return nil
}

func (b *BigQueryBackend) HandleUpdate(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error {
	start := time.Now()

	lrev, err := b.revForRepo(repo)
	if err != nil {
		return err
	}
	if lrev != "" {
		if rev < lrev {
			return nil
		}
	}

	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid path in HandleUpdate: %q", path)
	}
	col := parts[0]
	rkey := parts[1]

	defer func() {
		handleOpHist.WithLabelValues("update", col).Observe(float64(time.Since(start).Milliseconds()))
	}()

	switch col {
	case "app.bsky.feed.post":
		return fmt.Errorf("not allowed to update posts")
	default:
		if err := b.HandleUpdateGeneric(ctx, repo, col, rkey, rev, *rec, *cid); err != nil {
			return err
		}
	}

	return nil
}

type BQDeletion struct {
	URI        string    `bigquery:"uri"`        // The URI of the deleted record
	AuthorDID  string    `bigquery:"author_did"` // Who deleted it
	Collection string    `bigquery:"collection"` // What type of record it was
	DeletedAt  time.Time `bigquery:"deleted_at"` // When it was deleted
	Rkey       string    `bigquery:"rkey"`       // The rkey of the deleted record
}

func (b *BigQueryBackend) HandleDelete(ctx context.Context, repo string, rev string, path string) error {
	start := time.Now()
	defer func() {
		if parts := strings.Split(path, "/"); len(parts) > 0 {
			handleOpHist.WithLabelValues("delete", parts[0]).Observe(float64(time.Since(start).Milliseconds()))
		}
	}()

	// Revision check
	job, err := b.bfstore.GetJob(ctx, repo)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}
	if job.Rev() != "" && rev < job.Rev() {
		return nil
	}

	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid path in HandleDelete: %q", path)
	}

	col, rkey := parts[0], parts[1]
	uri := recordUri(repo, col, rkey)

	deletion := &BQDeletion{
		URI:        uri,
		AuthorDID:  repo,
		Collection: col,
		DeletedAt:  time.Now(),
		Rkey:       rkey,
	}

	inserter := b.dataset.Table("deletions").Inserter()
	if err := inserter.Put(ctx, deletion); err != nil {
		return fmt.Errorf("failed to insert deletion record: %w", err)
	}

	return nil
}

// Helper method to handle batch operations
func (b *BigQueryBackend) batchInsert(ctx context.Context, table string, items interface{}) error {
	inserter := b.dataset.Table(table).Inserter()
	if err := inserter.Put(ctx, items); err != nil {
		return fmt.Errorf("failed to batch insert to %s: %w", table, err)
	}
	return nil
}

func (b *BigQueryBackend) revForRepo(repo string) (string, error) {
	job, err := b.bfstore.GetJob(context.TODO(), repo)
	if err != nil {
		return "", nil
	}

	return job.Rev(), nil
}
