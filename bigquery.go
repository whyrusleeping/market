package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/backfill"
	"github.com/bluesky-social/indigo/util"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	"github.com/whyrusleeping/market/models"
	. "github.com/whyrusleeping/market/models"
	"google.golang.org/api/iterator"
)

// BigQueryBackend Handles interactions with BigQuery
type BigQueryBackend struct {
	s *Server

	bfstore *backfill.Gormstore

	client    *bigquery.Client
	dataset   *bigquery.Dataset
	projectID string
	datasetID string
	postCache *lru.TwoQueueCache[string, *cachedPostInfo]
	repoCache *lru.TwoQueueCache[string, *models.Repo]
	lastSeq   int64
	seqLk     sync.Mutex
}

// BQPost represents a post record in BigQuery
type BQPost struct {
	ID         string    `bigquery:"id"` // Composite key: did/rkey
	AuthorDID  string    `bigquery:"author_did"`
	Rkey       string    `bigquery:"rkey"`
	Created    time.Time `bigquery:"created"`
	Indexed    time.Time `bigquery:"indexed"`
	ReplyTo    string    `bigquery:"reply_to"` // References another post ID
	ReplyToDID string    `bigquery:"reply_to_did"`
	InThread   string    `bigquery:"in_thread"` // References root post ID
	Reposting  string    `bigquery:"reposting"` // For quote posts
	Raw        []byte    `bigquery:"raw"`
	NotFound   bool      `bigquery:"not_found"`
}

// BQRepo represents a repo record in BigQuery
type BQRepo struct {
	DID        string    `bigquery:"did"`
	Created    time.Time `bigquery:"created"`
	Indexed    time.Time `bigquery:"indexed"`
	Handle     string    `bigquery:"Handle"`
	ModifiedAt time.Time `bigquery:"modified_at"`
}

// BQLike represents a like record in BigQuery
type BQLike struct {
	ID        string    `bigquery:"id"` // Composite key: author_did/rkey
	AuthorDID string    `bigquery:"author_did"`
	Rkey      string    `bigquery:"rkey"`
	Created   time.Time `bigquery:"created"`
	Indexed   time.Time `bigquery:"indexed"`
	SubjectID string    `bigquery:"subject_id"` // References post ID
}

// BQRepost represents a repost record in BigQuery
type BQRepost struct {
	ID        string    `bigquery:"id"` // Composite key: author_did/rkey
	AuthorDID string    `bigquery:"author_did"`
	Rkey      string    `bigquery:"rkey"`
	Created   time.Time `bigquery:"created"`
	Indexed   time.Time `bigquery:"indexed"`
	SubjectID string    `bigquery:"subject_id"` // References post ID
}

// BQFollow represents a follow record in BigQuery
type BQFollow struct {
	ID         string    `bigquery:"id"` // Composite key: author_did/rkey
	AuthorDID  string    `bigquery:"author_did"`
	Rkey       string    `bigquery:"rkey"`
	Created    time.Time `bigquery:"created"`
	Indexed    time.Time `bigquery:"indexed"`
	SubjectDID string    `bigquery:"subject_did"`
}

// BQProfile represents a profile record in BigQuery
type BQProfile struct {
	DID     string    `bigquery:"did"`
	Created time.Time `bigquery:"created"`
	Indexed time.Time `bigquery:"indexed"`
	Raw     []byte    `bigquery:"raw"`
	Rev     string    `bigquery:"rev"`
}

// BQPostCounts represents aggregated counts for posts
type BQPostCounts struct {
	PostID     string    `bigquery:"post_id"`
	Likes      int64     `bigquery:"likes"`
	Replies    int64     `bigquery:"replies"`
	Reposts    int64     `bigquery:"reposts"`
	Quotes     int64     `bigquery:"quotes"`
	ThreadSize int64     `bigquery:"thread_size"`
	UpdatedAt  time.Time `bigquery:"updated_at"`
}

// Schema definitions for BigQuery tables
var (
	PostsSchema = bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType, Required: true},
		{Name: "author_did", Type: bigquery.StringFieldType, Required: true},
		{Name: "rkey", Type: bigquery.StringFieldType, Required: true},
		{Name: "created", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "indexed", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "reply_to", Type: bigquery.StringFieldType},
		{Name: "reply_to_did", Type: bigquery.StringFieldType},
		{Name: "in_thread", Type: bigquery.StringFieldType},
		{Name: "reposting", Type: bigquery.StringFieldType},
		{Name: "raw", Type: bigquery.BytesFieldType},
		{Name: "not_found", Type: bigquery.BooleanFieldType},
	}

	ReposSchema = bigquery.Schema{
		{Name: "did", Type: bigquery.StringFieldType, Required: true},
		{Name: "created", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "indexed", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "Handle", Type: bigquery.StringFieldType},
		{Name: "modified_at", Type: bigquery.TimestampFieldType},
	}

	LikesSchema = bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType, Required: true},
		{Name: "author_did", Type: bigquery.StringFieldType, Required: true},
		{Name: "rkey", Type: bigquery.StringFieldType, Required: true},
		{Name: "created", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "indexed", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "subject_id", Type: bigquery.StringFieldType, Required: true},
	}

	RepostsSchema = bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType, Required: true},
		{Name: "author_did", Type: bigquery.StringFieldType, Required: true},
		{Name: "rkey", Type: bigquery.StringFieldType, Required: true},
		{Name: "created", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "indexed", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "subject_id", Type: bigquery.StringFieldType, Required: true},
	}

	FollowsSchema = bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType, Required: true},
		{Name: "author_did", Type: bigquery.StringFieldType, Required: true},
		{Name: "rkey", Type: bigquery.StringFieldType, Required: true},
		{Name: "created", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "indexed", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "subject_did", Type: bigquery.StringFieldType, Required: true},
	}

	ProfilesSchema = bigquery.Schema{
		{Name: "did", Type: bigquery.StringFieldType, Required: true},
		{Name: "created", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "indexed", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "raw", Type: bigquery.BytesFieldType},
		{Name: "rev", Type: bigquery.StringFieldType},
	}

	PostCountsSchema = bigquery.Schema{
		{Name: "post_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "likes", Type: bigquery.IntegerFieldType},
		{Name: "replies", Type: bigquery.IntegerFieldType},
		{Name: "reposts", Type: bigquery.IntegerFieldType},
		{Name: "quotes", Type: bigquery.IntegerFieldType},
		{Name: "thread_size", Type: bigquery.IntegerFieldType},
		{Name: "updated_at", Type: bigquery.TimestampFieldType, Required: true},
	}
)

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

func (b *BigQueryBackend) HandleCreatePost(ctx context.Context, repo string, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedPost
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	post := &BQPost{
		ID:        postUri(repo, rkey),
		AuthorDID: repo,
		Rkey:      rkey,
		Created:   created.Time(),
		Indexed:   time.Now(),
		Raw:       recb,
	}

	if rec.Reply != nil && rec.Reply.Parent != nil {
		if rec.Reply.Root == nil {
			return fmt.Errorf("post reply had nil root")
		}

		parentUri, err := syntax.ParseATURI(rec.Reply.Parent.Uri)
		if err != nil {
			return fmt.Errorf("parsing reply parent: %w", err)
		}

		post.ReplyTo = parentUri.String()
		post.ReplyToDID = parentUri.Authority().String()

		// Update reply counts
		if err := b.incrementPostCount(ctx, parentUri.String(), "replies", 1); err != nil {
			return err
		}

		// Set thread reference
		rootUri := rec.Reply.Root.Uri
		threadID, err := b.getPostIDFromUri(ctx, rootUri)
		if err != nil {
			return fmt.Errorf("getting thread root: %w", err)
		}
		post.InThread = threadID

		// Update thread size
		if err := b.incrementPostCount(ctx, threadID, "thread_size", 1); err != nil {
			return err
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
			quotedID, err := b.getPostIDFromUri(ctx, quotedPostUri)
			if err != nil {
				return fmt.Errorf("getting quote subject: %w", err)
			}

			post.Reposting = quotedID

			if err := b.incrementPostCount(ctx, quotedID, "quotes", 1); err != nil {
				return err
			}
		}
	}

	inserter := b.dataset.Table("posts").Inserter()
	if err := inserter.Put(ctx, post); err != nil {
		return fmt.Errorf("failed to insert post: %w", err)
	}

	// Initialize post counts
	counts := &BQPostCounts{
		PostID:    post.ID,
		UpdatedAt: time.Now(),
	}
	if err := b.dataset.Table("post_counts").Inserter().Put(ctx, counts); err != nil {
		return fmt.Errorf("failed to initialize post counts: %w", err)
	}

	return nil
}

func (b *BigQueryBackend) HandleCreateLike(ctx context.Context, repo string, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedLike
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	subjectID, err := b.getPostIDFromUri(ctx, rec.Subject.Uri)
	if err != nil {
		return fmt.Errorf("getting like subject: %w", err)
	}

	like := &BQLike{
		ID:        likeUri(repo, rkey),
		AuthorDID: repo,
		Rkey:      rkey,
		Created:   created.Time(),
		Indexed:   time.Now(),
		SubjectID: subjectID,
	}

	inserter := b.dataset.Table("likes").Inserter()
	if err := inserter.Put(ctx, like); err != nil {
		return fmt.Errorf("failed to insert like: %w", err)
	}

	if err := b.incrementPostCount(ctx, subjectID, "likes", 1); err != nil {
		return err
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

	subjectID, err := b.getPostIDFromUri(ctx, rec.Subject.Uri)
	if err != nil {
		return fmt.Errorf("getting repost subject: %w", err)
	}

	repost := &BQRepost{
		ID:        repostUri(repo, rkey),
		AuthorDID: repo,
		Rkey:      rkey,
		Created:   created.Time(),
		Indexed:   time.Now(),
		SubjectID: subjectID,
	}

	inserter := b.dataset.Table("reposts").Inserter()
	if err := inserter.Put(ctx, repost); err != nil {
		return fmt.Errorf("failed to insert repost: %w", err)
	}

	if err := b.incrementPostCount(ctx, subjectID, "reposts", 1); err != nil {
		return err
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

func (b *BigQueryBackend) HandleCreateProfile(ctx context.Context, repo string, rkey string, rev string, recb []byte, cc cid.Cid) error {
	profile := &BQProfile{
		DID:     repo,
		Created: time.Now(),
		Indexed: time.Now(),
		Raw:     recb,
		Rev:     rev,
	}

	inserter := b.dataset.Table("profiles").Inserter()
	if err := inserter.Put(ctx, profile); err != nil {
		return fmt.Errorf("failed to insert profile: %w", err)
	}

	return nil
}

// Helper methods

func (b *BigQueryBackend) incrementPostCount(ctx context.Context, postID string, field string, delta int64) error {

	query := fmt.Sprintf(`
		UPDATE dataset.post_counts
		SET %s = %s + @delta,
		    updated_at = CURRENT_TIMESTAMP()
		WHERE post_id = @post_id
	`, field, field)

	q := b.client.Query(query)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "delta", Value: delta},
		{Name: "post_id", Value: postID},
	}

	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run update query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for job: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("job failed: %w", status.Err())
	}

	return nil
}

func (b *BigQueryBackend) getPostIDFromUri(ctx context.Context, uri string) (string, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s", puri.Did, puri.Rkey), nil
}

// Delete Handlers

func (b *BigQueryBackend) HandleDeletePost(ctx context.Context, repo string, rkey string) error {
	postID := postUri(repo, rkey)

	// First get the post to Handle decrements
	query := "SELECT * FROM dataset.posts WHERE id = @post_id"
	q := b.client.Query(query)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "post_id", Value: postID},
	}

	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to query post: %w", err)
	}

	var post BQPost
	if err := it.Next(&post); err != nil {
		return fmt.Errorf("failed to read post: %w", err)
	}

	// Decrement counts if needed
	if post.ReplyTo != "" {
		if err := b.incrementPostCount(ctx, post.ReplyTo, "replies", -1); err != nil {
			return err
		}
	}
	if post.InThread != "" {
		if err := b.incrementPostCount(ctx, post.InThread, "thread_size", -1); err != nil {
			return err
		}
	}
	if post.Reposting != "" {
		if err := b.incrementPostCount(ctx, post.Reposting, "quotes", -1); err != nil {
			return err
		}
	}

	// Delete the post itself using DML
	deleteQuery := "DELETE FROM dataset.posts WHERE id = @post_id"
	q = b.client.Query(deleteQuery)
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

func (b *BigQueryBackend) HandleDeleteLike(ctx context.Context, repo string, rkey string) error {
	likeID := likeUri(repo, rkey)

	// First get the like to get the subject ID
	query := "SELECT * FROM dataset.likes WHERE id = @like_id"
	q := b.client.Query(query)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "like_id", Value: likeID},
	}

	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to query like: %w", err)
	}

	var like BQLike
	if err := it.Next(&like); err != nil {
		return fmt.Errorf("failed to read like: %w", err)
	}

	// Decrement the like count
	if err := b.incrementPostCount(ctx, like.SubjectID, "likes", -1); err != nil {
		return err
	}

	// Delete the like
	deleteQuery := "DELETE FROM dataset.likes WHERE id = @like_id"
	q = b.client.Query(deleteQuery)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "like_id", Value: likeID},
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

	deleteQuery := "DELETE FROM dataset.follows WHERE id = @follow_id"
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

func (b *BigQueryBackend) HandleDeleteProfile(ctx context.Context, repo string, rkey string) error {
	deleteQuery := "DELETE FROM dataset.profiles WHERE did = @did"
	q := b.client.Query(deleteQuery)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "did", Value: repo.Did},
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
func (b *BigQueryBackend) HandleUpdateProfile(ctx context.Context, repo string, rkey string, rev string, recb []byte, cc cid.Cid) error {
	profile := &BQProfile{
		DID:     repo.Did,
		Created: time.Now(),
		Indexed: time.Now(),
		Raw:     recb,
		Rev:     rev,
	}

	// In BigQuery, we'll just insert a new row for the profile update
	// The query layer can Handle getting the latest profile by DID
	inserter := b.dataset.Table("profiles").Inserter()
	if err := inserter.Put(ctx, profile); err != nil {
		return fmt.Errorf("failed to insert updated profile: %w", err)
	}

	return nil
}

// Main Handle methods that route to specific Handlers
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
		if err := b.HandleCreatePost(ctx, repo, rkey, *rec, *cid); err != nil {
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
	case "app.bsky.actor.profile":
		if err := b.HandleCreateProfile(ctx, repo, rkey, rev, *rec, *cid); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized record type: %q", col)
	}

	return nil
}

func (b *BigQueryBackend) HandleUpdate(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error {
	start := time.Now()

	rr, err := b.getOrCreateRepo(ctx, repo)
	if err != nil {
		return fmt.Errorf("get user failed: %w", err)
	}

	lrev, err := b.revForRepo(rr)
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
	case "app.bsky.actor.profile":
		if err := b.HandleUpdateProfile(ctx, rr, rkey, rev, *rec, *cid); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized record type for update: %q", col)
	}

	return nil
}

func (b *BigQueryBackend) HandleDelete(ctx context.Context, repo string, rev string, path string) error {
	start := time.Now()

	rr, err := b.getOrCreateRepo(ctx, repo)
	if err != nil {
		return fmt.Errorf("get user failed: %w", err)
	}

	job, err := b.bfstore.GetJob(ctx, rr.Did)
	if err != nil {
		return err
	}
	if job.Rev() != "" {
		if rev < job.Rev() {
			return nil
		}
	}

	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid path in HandleDelete: %q", path)
	}
	col := parts[0]
	rkey := parts[1]

	defer func() {
		handleOpHist.WithLabelValues("delete", col).Observe(float64(time.Since(start).Milliseconds()))
	}()

	switch col {
	case "app.bsky.feed.post":
		if err := b.HandleDeletePost(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.feed.like":
		if err := b.HandleDeleteLike(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.feed.repost":
		if err := b.HandleDeleteRepost(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.follow":
		if err := b.HandleDeleteFollow(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.actor.profile":
		if err := b.HandleDeleteProfile(ctx, rr, rkey); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized record type for delete: %q", col)
	}

	return nil
}

// Helper methods for managing posts and repos
func (b *BigQueryBackend) getOrCreateRepo(ctx context.Context, did string) (*Repo, error) {
	r, ok := b.repoCache.Get(did)
	if ok {
		return r, nil
	}

	query := "SELECT * FROM dataset.repos WHERE did = @did"
	q := b.client.Query(query)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "did", Value: did},
	}

	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query repo: %w", err)
	}

	var bqRepo BQRepo
	err = it.Next(&bqRepo)
	if err != nil && err != iterator.Done {
		return nil, fmt.Errorf("failed to read repo: %w", err)
	}

	if err == iterator.Done {
		// Repo doesn't exist, create it
		bqRepo = BQRepo{
			DID:     did,
			Created: time.Now(),
			Indexed: time.Now(),
		}

		inserter := b.dataset.Table("repos").Inserter()
		if err := inserter.Put(ctx, &bqRepo); err != nil {
			return nil, fmt.Errorf("failed to insert repo: %w", err)
		}
	}

	// Convert to internal Repo type
	repo := &Repo{
		Did:   did,
		Setup: true,
	}

	b.repoCache.Add(did, repo)
	return repo, nil
}

func (b *BigQueryBackend) HandleDeleteRepost(ctx context.Context, repo string, rkey string) error {
	repostID := fmt.Sprintf("%s/%s", repo.Did, rkey)

	// Get the repost to find the subject
	query := "SELECT * FROM dataset.reposts WHERE id = @repost_id"
	q := b.client.Query(query)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "repost_id", Value: repostID},
	}

	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to query repost: %w", err)
	}

	var repost BQRepost
	if err := it.Next(&repost); err != nil {
		return fmt.Errorf("failed to read repost: %w", err)
	}

	// Decrement the repost count
	if err := b.incrementPostCount(ctx, repost.SubjectID, "reposts", -1); err != nil {
		return err
	}

	// Delete the repost
	deleteQuery := "DELETE FROM dataset.reposts WHERE id = @repost_id"
	q = b.client.Query(deleteQuery)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "repost_id", Value: repostID},
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

func (b *BigQueryBackend) handleDeleteFollow(ctx context.Context, repo string, rkey string) error {
	followID := fmt.Sprintf("%s/%s", repo.Did, rkey)

	// Delete the follow
	query := "DELETE FROM dataset.follows WHERE id = @follow_id"
	q := b.client.Query(query)
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

func (b *BigQueryBackend) handleDeleteProfile(ctx context.Context, repo string, rkey string) error {
	// Delete the profile
	query := "DELETE FROM dataset.profiles WHERE did = @did"
	q := b.client.Query(query)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "did", Value: repo.Did},
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

// Update handlers

func (b *BigQueryBackend) handleUpdateProfile(ctx context.Context, repo string, rkey string, rev string, recb []byte, cc cid.Cid) error {
	// In BigQuery, we'll treat updates as new insertions with a timestamp
	profile := &BQProfile{
		DID:     repo.Did,
		Created: time.Now(),
		Indexed: time.Now(),
		Raw:     recb,
		Rev:     rev,
	}

	inserter := b.dataset.Table("profiles").Inserter()
	if err := inserter.Put(ctx, profile); err != nil {
		return fmt.Errorf("failed to update profile: %w", err)
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

// Method to run aggregation jobs
func (b *BigQueryBackend) runAggregationJobs(ctx context.Context) error {
	// Example aggregation query to update post counts
	query := `
		UPDATE dataset.post_counts pc
		SET 
			likes = (
				SELECT COUNT(*)
				FROM dataset.likes l
				WHERE l.subject_id = pc.post_id
			),
			reposts = (
				SELECT COUNT(*)
				FROM dataset.reposts r
				WHERE r.subject_id = pc.post_id
			),
			replies = (
				SELECT COUNT(*)
				FROM dataset.posts p
				WHERE p.reply_to = pc.post_id
			),
			quotes = (
				SELECT COUNT(*)
				FROM dataset.posts p
				WHERE p.reposting = pc.post_id
			),
			updated_at = CURRENT_TIMESTAMP()
		WHERE pc.updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
	`

	q := b.client.Query(query)
	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run aggregation job: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for aggregation job: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("aggregation job failed: %w", status.Err())
	}

	return nil
}

// Method to handle cursor management
func (b *BigQueryBackend) updateCursor(ctx context.Context, cursor int64) error {
	query := `
		UPDATE dataset.cursor
		SET value = @cursor,
		    updated_at = CURRENT_TIMESTAMP()
		WHERE id = 1
	`

	q := b.client.Query(query)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "cursor", Value: cursor},
	}

	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to update cursor: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for cursor update: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("cursor update failed: %w", status.Err())
	}

	return nil
}

// Method to load cursor
func (b *BigQueryBackend) loadCursor(ctx context.Context) (int64, error) {
	query := `
		SELECT value
		FROM dataset.cursor
		WHERE id = 1
	`

	q := b.client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to query cursor: %w", err)
	}

	var value int64
	err = it.Next(&value)
	if err != nil {
		return 0, fmt.Errorf("failed to read cursor value: %w", err)
	}

	return value, nil
}

// Method to handle batch deletes
func (b *BigQueryBackend) batchDelete(ctx context.Context, table string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	query := fmt.Sprintf(`
		DELETE FROM dataset.%s
		WHERE id IN UNNEST(@ids)
	`, table)

	q := b.client.Query(query)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "ids", Value: ids},
	}

	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run batch delete: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for batch delete: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("batch delete failed: %w", status.Err())
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
