package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	lru "github.com/hashicorp/golang-lru/v2"
	pgvector "github.com/pgvector/pgvector-go"
	. "github.com/whyrusleeping/market/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type embStore struct {
	db              *gorm.DB
	embeddingServer string
	s               *Server
	b               *PostgresBackend

	profileImageCache *lru.TwoQueueCache[string, []byte]
}

func NewEmbStore(db *gorm.DB, embserv string, s *Server, b *PostgresBackend) *embStore {
	c, _ := lru.New2Q[string, []byte](100_000)
	return &embStore{
		db:                db,
		embeddingServer:   embserv,
		s:                 s,
		b:                 b,
		profileImageCache: c,
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
}

type Embedding struct {
	Vector  []float32 `json:"vector"`
	ModelID string    `json:"model_id"`
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

func (s *embStore) CreatePostEmbedding(ctx context.Context, repo *Repo, postid uint, fp *bsky.FeedPost) error {
	start := time.Now()
	defer func() {
		doEmbedHist.WithLabelValues("post").Observe(time.Since(start).Seconds())
	}()
	emb, err := s.computePostEmbedding(ctx, repo, fp)
	if err != nil {
		return err
	}

	if err := s.db.Create(&postEmbedding{
		Post:      postid,
		Model:     emb.ModelID,
		Embedding: pgvector.NewVector(emb.Vector),
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *embStore) computePostEmbedding(ctx context.Context, r *Repo, fp *bsky.FeedPost) (*Embedding, error) {
	authorEmb, err := s.loadUserEmbedding(ctx, r.ID)
	if err != nil {
		return nil, err
	}

	peb := &postEmbedBody{
		Post:            fp,
		AuthorEmbedding: authorEmb,
		ReplyType:       "root",
	}

	if fp.Reply != nil && fp.Reply.Parent != nil {
		parentEmb, err := s.loadPostEmbedding(ctx, fp.Reply.Parent.Uri)
		if err != nil {
			return nil, err
		}

		if parentEmb != nil {
			peb.ParentEmbedding = parentEmb
			peb.ReplyType = "reply"
		}
	}

	if fp.Embed != nil && fp.Embed.EmbedImages != nil {
		for _, img := range fp.Embed.EmbedImages.Images {
			if img.Image != nil {
				imgb, _, err := s.getImage(ctx, r.Did, img.Image.Ref.String(), "feed_fullsize")
				if err != nil {
					return nil, err
				}

				peb.Images = append(peb.Images, &pictureObj{
					Cid:   img.Image.Ref.String(),
					Bytes: imgb,
				})
			}
		}
	}

	b, err := json.Marshal(peb)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", s.embeddingServer+"/embed/post", bytes.NewReader(b))
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
		fmt.Println("error on posts: ", string(ob))
		return nil, fmt.Errorf("bad response status from embedding server: %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	var out Embedding
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	return &out, nil
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

	if err := s.s.maybeFetchImage(uri, s.s.imageCacheDir); err != nil {
		return nil, "", err
	}

	buf := new(bytes.Buffer)
	if err := s.s.getImageFromCache(did, cid, buf, true); err != nil {
		return nil, "", err
	}

	if kind == "avatar" || kind == "banner" {
		s.profileImageCache.Add(uri, buf.Bytes())
	}

	return buf.Bytes(), uri, nil
}

func (s *embStore) CreateOrUpdateUserEmbedding(ctx context.Context, r *Repo) error {
	start := time.Now()
	defer func() {
		doEmbedHist.WithLabelValues("user").Observe(time.Since(start).Seconds())
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

	recentInteractions, err := s.getRecentUserInteractions(ctx, r)
	if err != nil {
		return err
	}

	interactions := averageEmbeddings(recentInteractions)

	emb, err := s.computeUserEmbedding(ctx, "", pfpBytes, headerBytes, description, name, interactions)
	if err != nil {
		return err
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

	return nil
}

func (s *embStore) getRecentUserInteractions(ctx context.Context, r *Repo) ([]Embedding, error) {
	var posts []postEmbedding
	if err := s.db.Raw("SELECT * FROM post_embeddings WHERE post IN (SELECT id FROM posts WHERE author = ? ORDER BY posts.rkey DESC limit 15)", r.ID).Scan(&posts).Error; err != nil {
		return nil, err
	}

	var likedPosts []postEmbedding
	if err := s.db.Raw("SELECT * FROM post_embeddings WHERE post IN (SELECT subject FROM likes WHERE likes.author = ? ORDER BY likes.rkey DESC limit 15)", r.ID).Scan(&likedPosts).Error; err != nil {
		return nil, err
	}

	if len(posts) > 10 {
		posts = posts[:10]
	}

	if len(likedPosts) > 10 {
		likedPosts = likedPosts[:10]
	}

	var out []Embedding
	for _, emb := range posts {
		out = append(out, Embedding{
			Vector:  emb.Embedding.Slice(),
			ModelID: emb.Model,
		})
	}

	for _, emb := range likedPosts {
		out = append(out, Embedding{
			Vector:  emb.Embedding.Slice(),
			ModelID: emb.Model,
		})
	}

	return out, nil
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
}

func (s *embStore) computeUserEmbedding(ctx context.Context, repo string, pfp, header *pictureObj, description, name string, interactions *Embedding) (*Embedding, error) {
	ueb := &userEmbedBody{
		ProfilePic:         pfp,
		HeaderPic:          header,
		Description:        description,
		Name:               name,
		RecentInteractions: interactions,
	}

	b, err := json.Marshal(ueb)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", s.embeddingServer+"/embed/user", bytes.NewReader(b))
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

	return &out, nil
}
