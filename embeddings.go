package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nfnt/resize"
	pgvector "github.com/pgvector/pgvector-go"
	"github.com/whyrusleeping/market/halfvec"
	. "github.com/whyrusleeping/market/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type embStore struct {
	db               *gorm.DB
	pgxdb            *pgxpool.Pool
	embeddingServers []embedServerConfig
	s                *Server
	b                *PostgresBackend

	profileImageCache *lru.TwoQueueCache[string, []byte]

	embedBackends []embedBackendConfig

	bufClLk                sync.Mutex
	bufferedClusterUpdates map[string]*clusterInfoWeights

	bufEmbLk               sync.Mutex
	bufferedPostEmbUpdates *addEmbeddingsBody
	bufferedUserEmbUpdates *addEmbeddingsBody

	clusterMappingsCache *lru.Cache[uint, *cachedClusterInfo]

	postUriCache *lru.TwoQueueCache[uint, string]

	vectoorHost string
}

type embedBackendConfig struct {
	Host string
	Key  string
}

type embedServerConfig struct {
	Host  string
	Model string
}

func NewEmbStore(db *gorm.DB, pgxdb *pgxpool.Pool, embservs []string, s *Server, b *PostgresBackend) *embStore {
	var es []embedServerConfig
	for _, h := range embservs {
		parts := strings.Split(h, "|")
		es = append(es, embedServerConfig{
			Host:  parts[0],
			Model: parts[1],
		})
	}
	c, _ := lru.New2Q[string, []byte](100_000)
	cmc, _ := lru.New[uint, *cachedClusterInfo](300_000)
	puc, _ := lru.New2Q[uint, string](500_000)
	return &embStore{
		db:                     db,
		pgxdb:                  pgxdb,
		embeddingServers:       es,
		s:                      s,
		b:                      b,
		profileImageCache:      c,
		bufferedClusterUpdates: make(map[string]*clusterInfoWeights),
		clusterMappingsCache:   cmc,
		postUriCache:           puc,
	}
}

/*
{'post': {'$type': 'app.bsky.feed.post',
  'createdAt': '2025-01-24T19:01:54.038Z',
  'embed': {'$type': 'app.bsky.embed.images',
   'images': [{'alt': '',
     'aspectRatio': {'height': 2000, 'width': 900},
     'image': {'$type': 'blob',
      'ref': {'$link': 'bafkreigdwgzbcru56akocrzqs2fdllpxb2u7qt6jeuon4ml5l25xoaip4y'},
      'mimeType': 'image/jpeg',
      'size': 339506}}]},
  'facets': [{'features': [{'$type': 'app.bsky.richtext.facet#mention',
      'did': 'did:plc:kvlpuxt5qxo53n3g3atjxmqs'}],
    'index': {'byteEnd': 76, 'byteStart': 49}}],
  'langs': ['es'],
  'text': 'Peroperopero 🥹🥹🥹🥹\nUn mol de gracias \n@pierrenodoyuna.bsky.social 💜💜💜 qué ilusión!!!'},
 'images': {'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:j2d573rpra3bu5j7zteamrf6/bafkreigdwgzbcru56akocrzqs2fdllpxb2u7qt6jeuon4ml5l25xoaip4y@jpeg': '/9j+/Z'},
 'reply_type': 'root',
 'author_embedding': {'vector': [-0.01220703125,
   0.07275390625,
   -0.0038604736328125],
  'model_id': 'as'},
 'parent_embedding': None}
*/

type postEmbedBody struct {
	Post            *bsky.FeedPost `json:"post"`
	Images          []*pictureObj  `json:"images"`
	ReplyType       string         `json:"reply_type"`
	AuthorEmbedding *Embedding     `json:"author_embedding"`
	ParentEmbedding *Embedding     `json:"parent_embedding"`
	Uri             string         `json:"uri"`
	AuthorDid       string         `json:"author_did"`
	ParentUri       string         `json:"parent_uri,omitempty"`
}

type Embedding struct {
	Vector   []float32       `json:"vector"`
	ModelID  string          `json:"model_id"`
	Clusters map[int]float64 `json:"clusters,omitempty"`
	Topic    *string         `json:"topic,omitempty"`
}

func (e *Embedding) PadVectorTo(n int) {
	if len(e.Vector) < n {
		e.Vector = append(e.Vector, make([]float32, n-len(e.Vector))...)
	}
}

var embeddingSizes = map[string]int{
	"blip2": 256,
}

func (e *Embedding) TrimPadding() {
	es, ok := embeddingSizes[e.ModelID]
	if !ok {
		return
	}

	if len(e.Vector) > es {
		e.Vector = e.Vector[:es]
	}
}

type userEmbedding struct {
	ID        uint
	CreatedAt time.Time
	UpdatedAt time.Time
	Repo      uint
	Model     string
	Embedding pgvector.Vector `gorm:"type:halfvec(512)"`
}

type postEmbedding struct {
	ID        uint
	CreatedAt time.Time
	UpdatedAt time.Time
	Post      uint
	Model     string
	Embedding pgvector.Vector `gorm:"type:halfvec(512)"`
}

func (s *embStore) loadUserEmbedding(ctx context.Context, repo uint) (*Embedding, error) {
	var ue userEmbedding
	if err := s.db.Raw("SELECT * FROM user_embeddings WHERE repo = ?", repo).Scan(&ue).Error; err != nil {
		return nil, err
	}

	if ue.ID == 0 {
		return nil, nil
	}

	return &Embedding{
		Vector:  ue.Embedding.Slice(),
		ModelID: ue.Model,
	}, nil
}

func (s *embStore) loadPostEmbedding(ctx context.Context, postUri string) (*Embedding, error) {
	pinfo, err := s.b.postInfoForUri(ctx, postUri)
	if err != nil {
		return nil, fmt.Errorf("getting post info: %w", err)
	}

	var pe postEmbedding
	if err := s.db.Raw("SELECT * FROM post_embeddings WHERE post = ?", pinfo.ID).Scan(&pe).Error; err != nil {
		return nil, err
	}

	if pe.ID == 0 {
		return nil, nil
	}

	return &Embedding{
		Vector:  pe.Embedding.Slice(),
		ModelID: pe.Model,
	}, nil
}

func (s *embStore) CreatePostEmbedding(ctx context.Context, repo *Repo, p *Post, fp *bsky.FeedPost) error {
	var wg sync.WaitGroup
	for _, h := range s.embeddingServers {
		wg.Add(1)
		go func(hh string) {
			defer wg.Done()
			if err := s.createPostEmbedding(ctx, hh, repo, p, fp); err != nil {
				slog.Error("failed to create post embedding", "host", hh, "error", err)
			}
		}(h.Host)
	}

	wg.Wait()
	return nil
}

func (s *embStore) createPostEmbedding(ctx context.Context, host string, repo *Repo, p *Post, fp *bsky.FeedPost) error {
	start := time.Now()
	defer func() {
		doEmbedHist.WithLabelValues("post").Observe(time.Since(start).Seconds())
	}()
	emb, err := s.computePostEmbedding(ctx, host, repo, p, fp)
	if err != nil {
		return fmt.Errorf("failed to compute emb: %w", err)
	}

	if err := s.db.Create(&postEmbedding{
		Post:      p.ID,
		Model:     emb.ModelID,
		Embedding: pgvector.NewVector(emb.Vector),
	}).Error; err != nil {
		return err
	}

	if err := s.pushRemotePostEmbedding(ctx, repo, p, emb); err != nil {
		slog.Error("failed to push post embedding", "error", err)
	}

	return nil
}

func (s *embStore) computePostEmbedding(ctx context.Context, host string, r *Repo, p *Post, fp *bsky.FeedPost) (*Embedding, error) {
	var postPrep time.Time
	start := time.Now()

	defer func() {
		took := time.Since(start)
		prepTook := postPrep.Sub(start)

		embeddingTimeHist.WithLabelValues("post", "total", host).Observe(took.Seconds())
		embeddingTimeHist.WithLabelValues("post", "prep", host).Observe(prepTook.Seconds())
	}()

	authorEmb, err := s.loadUserEmbedding(ctx, r.ID)
	if err != nil {
		return nil, fmt.Errorf("loading user embedding: %w", err)
	}

	peb := &postEmbedBody{
		Post:            fp,
		AuthorEmbedding: authorEmb,
		ReplyType:       "root",
		AuthorDid:       r.Did,
		Uri:             "at://" + r.Did + "/app.bsky.feed.post/" + p.Rkey,
	}

	if fp.Reply != nil && fp.Reply.Parent != nil {
		parentEmb, err := s.loadPostEmbedding(ctx, fp.Reply.Parent.Uri)
		if err != nil {
			return nil, fmt.Errorf("loading post embedding: %w", err)
		}

		if parentEmb != nil {
			peb.ParentEmbedding = parentEmb
			peb.ReplyType = "reply"
			peb.ParentUri = fp.Reply.Parent.Uri
		}
	}

	if fp.Embed != nil {
		if fp.Embed.EmbedImages != nil {
			for _, img := range fp.Embed.EmbedImages.Images {
				if img.Image != nil {
					imgb, _, err := s.getImage(ctx, r.Did, img.Image.Ref.String(), "feed_fullsize")
					if err != nil {
						return nil, fmt.Errorf("getting image: %w", err)
					}

					peb.Images = append(peb.Images, &pictureObj{
						Cid:   img.Image.Ref.String(),
						Bytes: imgb,
					})
				}
			}
		}
		if fp.Embed.EmbedVideo != nil {
			vid := fp.Embed.EmbedVideo

			imgb, err := s.getVideoThumbnail(ctx, r.Did, vid.Video.Ref.String())
			if err != nil {
				return nil, fmt.Errorf("getting video thumbnail: %w", err)
			}
			peb.Images = append(peb.Images, &pictureObj{
				Cid:   vid.Video.Ref.String(),
				Bytes: imgb,
			})
		}
	}

	postPrep = time.Now()

	b, err := json.Marshal(peb)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", host+"/embed/post", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding server errored: %w", err)
	}

	ob, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		fmt.Println("error on posts: ", string(ob))
		return nil, fmt.Errorf("bad response status from embedding server: %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	var out Embedding
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	out.PadVectorTo(512)

	return &out, nil
}

func (s *embStore) getVideoThumbnail(ctx context.Context, did, cid string) ([]byte, error) {
	var lastError error
	for i := 0; i < 3; i++ {
		b, err := s.getVideoThumbnailDirect(ctx, did, cid)
		if err == nil {
			return b, nil
		}

		slog.Warn("failed to get video thumnail", "attempt", i, "did", did, "cid", cid, "error", err)
		lastError = err

		time.Sleep(time.Second * time.Duration(i+1))
	}

	return nil, lastError
}

func (s *embStore) getVideoThumbnailDirect(ctx context.Context, did, cid string) ([]byte, error) {
	uri := fmt.Sprintf("https://video.bsky.app/watch/%s/%s/thumbnail.jpg", did, cid)

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)

	if rlbypass := os.Getenv("BSKY_RATELIMITBYPASS"); rlbypass != "" {
		req.Header.Set("x-ratelimit-bypass", rlbypass)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch error: %w", err)
	}

	if resp.StatusCode != 200 {
		if resp.StatusCode == 404 {
			return nil, fmt.Errorf("thumbnail not found")
		}
		return nil, fmt.Errorf("invalid status: %d", resp.StatusCode)
	}

	img, _, err := image.Decode(resp.Body)
	if err != nil {
		return nil, err
	}

	rimg := resize.Resize(224, 224, img, resize.Lanczos2)

	buf := new(bytes.Buffer)
	if err := jpeg.Encode(buf, rimg, nil); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (s *embStore) getImage(ctx context.Context, did string, cid string, kind string) ([]byte, string, error) {
	uri, err := s.s.uriForImage(did, cid, kind)
	if err != nil {
		return nil, "", err
	}

	if kind == "avatar" || kind == "banner" {
		b, ok := s.profileImageCache.Get(uri)
		if ok {
			return b, uri, nil
		}
	}

	if err := s.s.maybeFetchImage(ctx, uri, s.s.imageCacheDir); err != nil {
		return nil, "", err
	}

	buf := new(bytes.Buffer)
	if err := s.s.getImageFromCache(ctx, did, cid, buf, true); err != nil {
		return nil, "", err
	}

	if kind == "avatar" || kind == "banner" {
		s.profileImageCache.Add(uri, buf.Bytes())
	}

	return buf.Bytes(), uri, nil
}

func (s *embStore) CreateOrUpdateUserEmbedding(ctx context.Context, r *Repo) error {
	for _, h := range s.embeddingServers {
		if err := s.createOrUpdateUserEmbedding(ctx, h.Host, h.Model, r); err != nil {
			slog.Error("failed to update user embedding", "host", h, "error", err)
		}
	}
	return nil
}

func (s *embStore) createOrUpdateUserEmbedding(ctx context.Context, host, model string, r *Repo) error {
	start := time.Now()
	var postPrep time.Time
	defer func() {
		doEmbedHist.WithLabelValues("user").Observe(time.Since(start).Seconds())

		took := time.Since(start)
		prepTook := postPrep.Sub(start)

		embeddingTimeHist.WithLabelValues("user", "total", host).Observe(took.Seconds())
		embeddingTimeHist.WithLabelValues("user", "prep", host).Observe(prepTook.Seconds())
	}()

	var prof Profile
	if err := s.db.Raw("SELECT * FROM profiles WHERE repo = ? ORDER BY indexed DESC limit 1", r.ID).Scan(&prof).Error; err != nil {
		return err
	}

	var bp bsky.ActorProfile
	if len(prof.Raw) > 0 {
		if err := bp.UnmarshalCBOR(bytes.NewReader(prof.Raw)); err != nil {
			slog.Error("failed to unmarshal profile bytes", "error", err)
		}
	}

	var description, name string
	if bp.Description != nil {
		description = *bp.Description
	}
	if bp.DisplayName != nil {
		name = *bp.DisplayName
	}

	var pfpBytes, headerBytes *pictureObj

	if bp.Avatar != nil {
		imgb, _, err := s.getImage(ctx, r.Did, bp.Avatar.Ref.String(), "avatar")
		if err != nil {
			slog.Error("fetching avatar image failed", "did", r.Did, "cid", bp.Avatar.Ref.String(), "error", err)
			if strings.Contains(err.Error(), "non-200 response code: 404") {
				//TODO
				if err := s.s.refetchProfileForDid(ctx, r.Did); err != nil {
					slog.Error("got a 404 on avater fetch, then failed to refetch profile", "did", r.Did, "error", err)
				}

			}
		} else {
			pfpBytes = &pictureObj{
				Cid:   bp.Avatar.Ref.String(),
				Bytes: imgb,
			}
		}
	}

	if bp.Banner != nil {
		imgb, _, err := s.getImage(ctx, r.Did, bp.Banner.Ref.String(), "banner")
		if err != nil {
			slog.Error("fetching banner image failed", "did", r.Did, "cid", bp.Banner.Ref.String(), "error", err)
		} else {
			headerBytes = &pictureObj{
				Cid:   bp.Banner.Ref.String(),
				Bytes: imgb,
			}
		}
	}

	recentInteractions, uris, err := s.getRecentUserInteractions(ctx, r, model)
	if err != nil {
		return fmt.Errorf("get recent user interactions: %w", err)
	}

	interactions := averageEmbeddings(recentInteractions)

	postPrep = time.Now()

	emb, err := s.computeUserEmbedding(ctx, host, r.Did, pfpBytes, headerBytes, description, name, interactions, uris)
	if err != nil {
		return fmt.Errorf("computing embedding: %w", err)
	}

	if err := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "repo"}},
		DoUpdates: clause.AssignmentColumns([]string{"model", "embedding", "updated_at"}),
	}).Create(&userEmbedding{
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Repo:      r.ID,
		Model:     emb.ModelID,
		Embedding: pgvector.NewVector(emb.Vector),
	}).Error; err != nil {
		return err
	}

	if emb.Clusters != nil {
		if err := s.pushClusterUpdate(ctx, emb.ModelID, r, emb.Clusters); err != nil {
			slog.Error("failed to push cluster update", "did", r.Did, "error", err)
		}
	}

	if err := s.pushRemoteUserEmbedding(ctx, r, emb); err != nil {
		slog.Error("failed to push remote embedding", "did", r.Did, "error", err)
	}

	return nil
}

func (s *embStore) pushRemotePostEmbedding(ctx context.Context, r *Repo, p *Post, emb *Embedding) error {
	url := "at://" + r.Did + "/app.bsky.feed.post/" + p.Rkey

	s.bufEmbLk.Lock()
	if s.bufferedPostEmbUpdates == nil {
		s.bufferedPostEmbUpdates = &addEmbeddingsBody{
			ModelID: emb.ModelID,
			Embeddings: map[string]embedInfoPortable{
				url: embedInfoPortable{
					Vec:   emb.Vector,
					Topic: emb.Topic,
				},
			},
		}

	} else {
		s.bufferedPostEmbUpdates.Embeddings[url] = embedInfoPortable{
			Vec:   emb.Vector,
			Topic: emb.Topic,
		}
	}

	if len(s.bufferedPostEmbUpdates.Embeddings) < 40 {
		s.bufEmbLk.Unlock()
		return nil
	}

	toSend := s.bufferedPostEmbUpdates
	s.bufferedPostEmbUpdates = nil
	s.bufEmbLk.Unlock()

	for _, be := range s.embedBackends {
		if err := s.sendEmbeddingBatch(ctx, be, toSend); err != nil {
			slog.Error("failed to push embedding batch", "backend", be.Host, "batchSize", len(toSend.Embeddings), "error", err)
		}
	}

	return nil
}

func (s *embStore) sendEmbeddingBatch(ctx context.Context, be embedBackendConfig, toSend *addEmbeddingsBody) error {
	b, err := json.Marshal(toSend)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", be.Host+"/admin/addPostEmbeddings", bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+be.Key)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("non-200 status code from post embedding updates: %d - %s", resp.StatusCode, string(bb))
	}

	return nil
}

func (s *embStore) pushRemoteUserEmbedding(ctx context.Context, r *Repo, emb *Embedding) error {
	s.bufEmbLk.Lock()
	if s.bufferedUserEmbUpdates == nil {
		s.bufferedUserEmbUpdates = &addEmbeddingsBody{
			ModelID: emb.ModelID,
			Embeddings: map[string]embedInfoPortable{
				r.Did: embedInfoPortable{
					Vec: emb.Vector,
				},
			},
		}

	} else {
		s.bufferedUserEmbUpdates.Embeddings[r.Did] = embedInfoPortable{
			Vec: emb.Vector,
		}
	}

	if len(s.bufferedUserEmbUpdates.Embeddings) < 40 {
		s.bufEmbLk.Unlock()
		return nil
	}

	toSend := s.bufferedUserEmbUpdates
	s.bufferedUserEmbUpdates = nil
	s.bufEmbLk.Unlock()

	for _, be := range s.embedBackends {
		if err := s.sendUserEmbeddings(ctx, be, toSend); err != nil {
			slog.Error("failed to send embeddings to backend", "host", be.Host, "error", err)
		}
	}

	return nil
}

func (s *embStore) sendUserEmbeddings(ctx context.Context, be embedBackendConfig, toSend *addEmbeddingsBody) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	b, err := json.Marshal(toSend)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", be.Host+"/admin/addUserEmbeddings", bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+be.Key)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("non-200 status code from embedding updates: %d - %s", resp.StatusCode, string(bb))
	}

	return nil
}

type cachedClusterInfo struct {
	cachedAt time.Time
	vals     *clusterInfoWeights
}

type clusterInfo struct {
	Primary   int   `json:"p"`
	Interests []int `json:"i"`
}

type clusterInfoWeights struct {
	Primary   int             `json:"p"`
	Interests map[int]float64 `json:"i"`
}

type updateClustersBody struct {
	Assignments map[string]*clusterInfoWeights
	ModelID     string
}

func (s *embStore) newClusterInfoSimilar(ctx context.Context, r *Repo, clinfo *clusterInfoWeights) (bool, error) {
	cached, ok := s.clusterMappingsCache.Get(r.ID)
	if !ok {
		return false, nil
	}

	if time.Since(cached.cachedAt) > time.Hour {
		return false, nil
	}

	if cached.vals.Primary != clinfo.Primary {
		return false, nil
	}

	interests := make(map[int]bool)

	for in := range clinfo.Interests {
		interests[in] = true
	}

	var diff int
	for in := range cached.vals.Interests {
		if !interests[in] {
			diff++
		}
		delete(interests, in)
	}

	if diff+len(interests) > 3 {
		return true, nil
	}

	return false, nil
}

func (s *embStore) pushClusterUpdate(ctx context.Context, model string, r *Repo, clmap map[int]float64) error {
	start := time.Now()
	defer func() {
		took := time.Since(start)
		if took > time.Millisecond*300 {
			slog.Warn("cluster update finished", "took", time.Since(start))
		}
	}()
	if len(clmap) == 0 {
		return nil
	}

	var clinfo clusterInfoWeights
	var biggest int
	var biggestVal float64

	for k, v := range clmap {
		if v > biggestVal {
			biggest = k
			biggestVal = v
		}
	}

	clinfo.Primary = biggest
	clinfo.Interests = clmap

	tooSimilar, err := s.newClusterInfoSimilar(ctx, r, &clinfo)
	if err != nil {
		return err
	}

	if tooSimilar {
		return nil
	}

	s.bufClLk.Lock()

	s.bufferedClusterUpdates[r.Did] = &clinfo

	if len(s.bufferedClusterUpdates) < 40 {
		s.bufClLk.Unlock()
		return nil
	}

	toSend := s.bufferedClusterUpdates
	s.bufferedClusterUpdates = make(map[string]*clusterInfoWeights)
	s.bufClLk.Unlock()

	model = strings.TrimPrefix(model, "bsky_user_")
	body := &updateClustersBody{
		ModelID:     model,
		Assignments: toSend,
	}

	for _, be := range s.embedBackends {
		if err := s.sendClusterUpdates(ctx, be, body); err != nil {
			slog.Error("failed to push cluster updates to backend", "backend", be.Host, "error", err)
		}
	}

	return nil
}

func (s *embStore) sendClusterUpdates(ctx context.Context, be embedBackendConfig, body *updateClustersBody) error {

	b, err := json.Marshal(body)
	if err != nil {
		return err
	}

	for i := 0; i < 5; i++ {
		req, err := http.NewRequest("POST", be.Host+"/admin/clusterUpdates", bytes.NewReader(b))
		if err != nil {
			return err
		}

		req.Header.Add("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+be.Key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			if i == 4 {
				return err
			}
			slog.Error("failed to perform http request to cluster updates endpoint", "error", err)
			time.Sleep(time.Second*time.Duration(i) + (time.Duration(rand.IntN(2000)) * time.Millisecond))
			continue
		}
		defer resp.Body.Close()

		bb, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			return fmt.Errorf("non-200 status code from cluster update: %d - %s", resp.StatusCode, string(bb))
		}

		io.Copy(io.Discard, resp.Body)

		return nil
	}

	return fmt.Errorf("failed to make successful request")
}

func readEmbeddingRows(rows pgx.Rows) ([]halfvec.HalfVector, string, error) {
	defer rows.Close()

	var vals []halfvec.HalfVector
	var model string
	for rows.Next() {
		var embeddingData halfvec.HalfVector

		if err := rows.Scan(&embeddingData, &model); err != nil {
			return nil, "", fmt.Errorf("error scanning raw data: %w", err)
		}

		vals = append(vals, embeddingData)
	}

	return vals, model, nil
}

func (s *embStore) uriForPostID(ctx context.Context, pid uint) (string, error) {
	val, ok := s.postUriCache.Get(pid)
	if ok {
		return val, nil
	}

	var did, rkey string
	if err := s.pgxdb.QueryRow(ctx, "SELECT (SELECT did FROM repos WHERE id = posts.author) as did, rkey FROM posts WHERE id = $1", pid).Scan(&did, &rkey); err != nil {
		return "", err
	}

	uri := "at://" + did + "/app.bsky.feed.post/" + rkey

	s.postUriCache.Add(pid, uri)

	return uri, nil
}

func (s *embStore) getRecentUserInteractions(ctx context.Context, r *Repo, model string) ([]Embedding, []string, error) {
	/*
		rows, err := s.pgxdb.Query(ctx, "SELECT embedding, model FROM post_embeddings WHERE post IN (SELECT id FROM posts WHERE author = ? ORDER BY posts.rkey DESC limit 15)", r.ID)
		if err != nil {
			return nil, err
		}

		posts, pmodel, err := readEmbeddingRows(rows)
		if err != nil {
			return nil, err
		}
	*/
	var posts []postEmbedding
	if err := s.db.Raw("SELECT * FROM post_embeddings WHERE model = ? AND post IN (SELECT id FROM posts WHERE author = ? ORDER BY posts.rkey DESC limit 15)", model, r.ID).Scan(&posts).Error; err != nil {
		return nil, nil, err
	}

	/*
		rows, err = s.pgxdb.Query(ctx, "SELECT embedding, model FROM post_embeddings WHERE post IN (SELECT subject FROM likes WHERE likes.author = ? ORDER BY likes.rkey DESC limit 15)", r.ID)
		if err != nil {
			return nil, err
		}


		likedPosts, _, err := readEmbeddingRows(rows)
		if err != nil {
			return nil, err
		}
	*/
	var likedPosts []postEmbedding
	if err := s.db.Raw("SELECT * FROM post_embeddings WHERE model = ? AND post IN (SELECT subject FROM likes WHERE likes.author = ? ORDER BY likes.rkey DESC limit 15)", model, r.ID).Scan(&likedPosts).Error; err != nil {
		return nil, nil, err
	}

	if len(posts) > 10 {
		posts = posts[:10]
	}

	if len(likedPosts) > 10 {
		likedPosts = likedPosts[:10]
	}

	var uris []string
	var out []Embedding
	for _, emb := range posts {
		puri, err := s.uriForPostID(ctx, emb.Post)
		if err != nil {
			return nil, nil, err
		}
		uris = append(uris, puri)
		e := Embedding{
			Vector:  emb.Embedding.Slice(),
			ModelID: emb.Model,
		}
		e.TrimPadding()
		out = append(out, e)
	}

	for _, emb := range likedPosts {
		puri, err := s.uriForPostID(ctx, emb.Post)
		if err != nil {
			return nil, nil, err
		}
		uris = append(uris, puri)
		e := Embedding{
			Vector:  emb.Embedding.Slice(),
			ModelID: emb.Model,
		}
		e.TrimPadding()
		out = append(out, e)
	}

	return out, uris, nil
}

// averageEmbeddings calculates the element-wise average of multiple embeddings using SIMD when available
func averageEmbeddings(embs []Embedding) *Embedding {
	if len(embs) == 0 {
		return nil
	}

	if len(embs) == 1 {
		return &embs[0]
	}

	dim := len(embs[0].Vector)

	result := make([]float32, dim)

	count := float32(len(embs))

	for _, emb := range embs {
		for i := 0; i < dim; i++ {
			result[i] += emb.Vector[i]
		}
	}

	for i := 0; i < dim; i++ {
		result[i] /= count
	}

	return &Embedding{Vector: result, ModelID: embs[0].ModelID}
}

/*
{'profile_pic': '/9j/2wCEA',
  'header_pic': None,
  'description': '',
  'name': '',
  'recent_interactions': None},
*/

type pictureObj struct {
	Cid   string `json:"cid"`
	Bytes []byte `json:"bytes"`
}
type userEmbedBody struct {
	ProfilePic         *pictureObj `json:"profile_pic"`
	HeaderPic          *pictureObj `json:"header_pic"`
	Description        string      `json:"description"`
	Name               string      `json:"name"`
	RecentInteractions *Embedding  `json:"recent_interactions"`
	Did                string      `json:"did"`
}

func (s *embStore) computeUserEmbedding(ctx context.Context, embhost, repo string, pfp, header *pictureObj, description, name string, interactions *Embedding, interuri []string) (*Embedding, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	ueb := &userEmbedBody{
		ProfilePic:         pfp,
		HeaderPic:          header,
		Description:        description,
		Name:               name,
		RecentInteractions: interactions,
		Did:                repo,
	}

	b, err := json.Marshal(ueb)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", embhost+"/embed/user", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		ob, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		fmt.Println("error on user: ", string(ob))
		return nil, fmt.Errorf("bad response status from embedding server: %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	var out Embedding
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	out.PadVectorTo(512)

	return &out, nil
}

type embedInfoPortable struct {
	Vec   []float32
	Topic *string
}

type addEmbeddingsBody struct {
	ModelID    string
	Embeddings map[string]embedInfoPortable
}

type missingEmbsResponse struct {
	Posts []string
	Users []string
}

type dlqStats struct {
	PostsComputed int
	PostsFailed   int
	UsersComputed int
	UsersFailed   int
}

func (s *embStore) processDeadLetterQueue(ctx context.Context, be embedBackendConfig) (*dlqStats, error) {
	req, err := http.NewRequest("GET", s.vectoorHost+"/missingEmbeddings", nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("got non-200 status from backend for dead letter queue (%d): %s", resp.StatusCode, string(b))
	}

	var body missingEmbsResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}

	limiter := make(chan struct{}, 8)

	var flk sync.Mutex
	var failPosts, failUsers []string

	slog.Info("dead letter queue sizes", "posts", len(body.Posts), "users", len(body.Users))
	for _, p := range body.Posts {
		limiter <- struct{}{}
		go func(puri string) {
			if err := s.refreshPostEmbByUri(ctx, puri); err != nil {
				slog.Error("failed to refresh post emb", "uri", puri, "error", err)
				flk.Lock()
				failPosts = append(failPosts, puri)
				flk.Unlock()
			} else {
				//slog.Info("post embedding refreshed", "uri", puri)
			}
			<-limiter
		}(p)
	}

	for _, u := range body.Users {
		limiter <- struct{}{}
		go func(did string) {
			if err := s.refreshUserByDid(ctx, did); err != nil {
				slog.Error("failed to refresh user emb", "did", did, "error", err)
				flk.Lock()
				failUsers = append(failUsers, did)
				flk.Unlock()
			} else {
				//slog.Info("user embedding refreshed", "uri", did)
			}
			<-limiter
		}(u)
	}

	for i := 0; i < 8; i++ {
		limiter <- struct{}{}
	}

	if err := s.markEmbeddingsAsFailed(ctx, failPosts, failUsers); err != nil {
		slog.Error("failed to mark embeddings as failed", "error", err)
	}

	return &dlqStats{
		PostsComputed: len(body.Posts) - len(failPosts),
		PostsFailed:   len(failPosts),
		UsersComputed: len(body.Users) - len(failUsers),
		UsersFailed:   len(failUsers),
	}, nil
}

type missingEmbBody struct {
	FailedPosts []string
	FailedUsers []string
}

func (s *embStore) markEmbeddingsAsFailed(ctx context.Context, posts, users []string) error {
	b, err := json.Marshal(missingEmbBody{
		FailedPosts: posts,
		FailedUsers: users,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", s.vectoorHost+"/missingEmbeddings", bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("non-200 status code: %d", resp.StatusCode)
	}

	_, _ = io.Copy(io.Discard, resp.Body)

	return nil
}

func (s *embStore) refreshUserByDid(ctx context.Context, did string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	r, err := s.b.getOrCreateRepo(ctx, did)
	if err != nil {
		return err
	}

	if err := s.CreateOrUpdateUserEmbedding(ctx, r); err != nil {
		return err
	}
	return nil
}

func (s *embStore) refreshPostEmbByUri(ctx context.Context, uri string) error {
	var outErr error
	for _, h := range s.embeddingServers {
		if err := s.refreshPostEmbByUriOnHost(ctx, h.Host, uri); err != nil {
			outErr = fmt.Errorf("refresh failed on host: %q: %w", h, err)
		}
	}
	return outErr
}

func (s *embStore) refreshPostEmbByUriOnHost(ctx context.Context, host, uri string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	puri, err := syntax.ParseATURI(uri)
	if err != nil {
		return err
	}

	r, err := s.b.getOrCreateRepo(ctx, puri.Authority().String())
	if err != nil {
		return err
	}

	p, err := s.b.getPostByUri(ctx, uri, "*")
	if err != nil {
		return err
	}

	if p.NotFound || len(p.Raw) == 0 {
		npb, err := s.s.refetchPostByUri(ctx, uri)
		if err != nil {
			return fmt.Errorf("refetch post record failed: %w", err)
		}

		p.Raw = npb
	}

	var fp bsky.FeedPost
	if err := fp.UnmarshalCBOR(bytes.NewReader(p.Raw)); err != nil {
		return fmt.Errorf("unmarshal failed: %w", err)
	}

	return s.refreshPostEmbedding(ctx, host, r, p, &fp)
}

func (s *embStore) refreshPostEmbedding(ctx context.Context, host string, repo *Repo, p *Post, fp *bsky.FeedPost) error {
	start := time.Now()
	defer func() {
		doEmbedHist.WithLabelValues("post").Observe(time.Since(start).Seconds())
	}()

	var existing postEmbedding
	if err := s.db.Raw("SELECT * FROM post_embeddings WHERE post = ?", p.ID).Scan(&existing).Error; err != nil {
		return err
	}

	if existing.ID != 0 {
		// already have an embedding for this post, send it up
		oemb := &Embedding{
			Vector:  existing.Embedding.Slice(),
			ModelID: existing.Model,
		}

		if err := s.pushRemotePostEmbedding(ctx, repo, p, oemb); err != nil {
			slog.Error("failed to push refreshed post embedding", "error", err)
		}
		return nil
	}

	emb, err := s.computePostEmbedding(ctx, host, repo, p, fp)
	if err != nil {
		return fmt.Errorf("failed to compute emb: %w", err)
	}

	res := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "post"}, {Name: "model"}},
		DoNothing: true,
	}).Create(&postEmbedding{
		Post:      p.ID,
		Model:     emb.ModelID,
		Embedding: pgvector.NewVector(emb.Vector),
	})
	if err := res.Error; err != nil {
		return err
	}

	if res.RowsAffected == 0 {
		return nil
	}

	if err := s.pushRemotePostEmbedding(ctx, repo, p, emb); err != nil {
		slog.Error("failed to push post embedding", "error", err)
	}

	return nil
}
