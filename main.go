package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/backfill"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/autoscaling"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/util/cliutil"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gorilla/websocket"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	"github.com/nfnt/resize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

var handleOpHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "handle_op_duration",
	Help:    "A histogram of op handling durations",
	Buckets: prometheus.ExponentialBuckets(1, 2, 15),
}, []string{"op", "collection"})

func main() {
	app := cli.App{
		Name: "market",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "image-dir",
			Value: "image-cache",
		},
		&cli.StringFlag{
			Name:    "db-url",
			EnvVars: []string{"DATABASE_URL"},
		},
		&cli.IntFlag{
			Name:  "max-db-connections",
			Value: 50,
		},
		&cli.StringFlag{
			Name: "image-cache-server",
		},
	}
	app.Action = func(cctx *cli.Context) error {

		db, err := cliutil.SetupDatabase(cctx.String("db-url"), cctx.Int("max-db-connections"))
		if err != nil {
			return err
		}

		db.Logger = logger.New(log.New(os.Stdout, "\r\n", log.LstdFlags), logger.Config{
			SlowThreshold:             500 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: false,
			Colorful:                  true,
		})

		db.AutoMigrate(backfill.GormDBJob{})
		db.AutoMigrate(Repo{})
		db.AutoMigrate(Post{})
		db.AutoMigrate(PostCounts{})
		db.AutoMigrate(PostCountsTask{})
		db.AutoMigrate(Follow{})
		db.AutoMigrate(Block{})
		db.AutoMigrate(Like{})
		db.AutoMigrate(Repost{})
		db.AutoMigrate(List{})
		db.AutoMigrate(ListItem{})
		db.AutoMigrate(ListBlock{})
		db.AutoMigrate(Profile{})
		db.AutoMigrate(ThreadGate{})
		db.AutoMigrate(FeedGenerator{})
		db.AutoMigrate(cursorRecord{})
		db.AutoMigrate(MarketConfig{})
		db.AutoMigrate(Image{})

		rc, _ := lru.New2Q[string, *Repo](1_000_000)
		pc, _ := lru.New2Q[string, *cachedPostInfo](5_000_000)
		revc, _ := lru.New2Q[uint, string](1_000_000)
		s := &Server{
			db:               db,
			repoCache:        rc,
			postInfoCache:    pc,
			revCache:         revc,
			imageCacheDir:    cctx.String("image-dir"),
			imageCacheServer: cctx.String("image-cache-server"),
		}

		curs, err := s.loadCursor()
		if err != nil {
			return err
		}

		go s.syncCursorRoutine()

		go s.imageFetcher()

		gstore := backfill.NewGormstore(db)

		ctx := context.TODO()
		if err := gstore.LoadJobs(ctx); err != nil {
			return err
		}

		opts := backfill.DefaultBackfillOptions()
		opts.CheckoutPath = "https://bsky.network/xrpc/com.atproto.sync.getRepo"
		opts.SyncRequestsPerSecond = 20
		opts.ParallelBackfills = 50

		bf := backfill.NewBackfiller("market", gstore, s.handleCreate, s.handleUpdate, s.handleDelete, opts)
		s.bf = bf
		s.store = gstore

		go bf.Start()

		go s.runCountAggregator()

		go func() {
			if err := s.maybePumpRepos(context.TODO()); err != nil {
				slog.Error("backfill pump failed", "err", err)
			}
		}()

		if s.imagesEnabled() {
			go func() {
				if err := s.crawlOldPostsForPictures(); err != nil {
					slog.Error("backfill pump failed", "err", err)
				}
			}()
		}

		if err := s.startLiveTail(curs); err != nil {
			slog.Error("failed to start live tail", "err", err)
			return err
		}

		streamClosed := make(chan struct{})
		streamCtx, streamCancel := context.WithCancel(context.Background())
		go func() {
			if err := events.HandleRepoStream(streamCtx, s.con, s.eventScheduler); err != nil {
				slog.Error("repo stream failed", "err", err)
			}
			close(streamClosed)
		}()

		quit := make(chan struct{})
		exitSignals := make(chan os.Signal, 1)
		signal.Notify(exitSignals, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			select {
			case sig := <-exitSignals:
				slog.Info("received OS exit signal", "signal", sig)
			case <-streamClosed:
			}

			bf.Stop(context.TODO())

			// Shutdown the ingester.
			streamCancel()
			<-streamClosed

			if err := s.flushCursor(); err != nil {
				slog.Error("final flush cursor failed", "err", err)
			}

			// Trigger the return that causes an exit.
			close(quit)
		}()

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			http.HandleFunc("/images/", s.handleServeImage)
			http.ListenAndServe(":5151", nil)

		}()

		<-quit

		return nil
	}

	app.RunAndExitOnError()
}

type Server struct {
	db    *gorm.DB
	bf    *backfill.Backfiller
	store *backfill.Gormstore

	lastSeq int64
	seqLk   sync.Mutex

	con *websocket.Conn

	eventScheduler events.Scheduler
	streamFinished chan struct{}

	repoCache *lru.TwoQueueCache[string, *Repo]
	reposLk   sync.Mutex

	postInfoCache *lru.TwoQueueCache[string, *cachedPostInfo]

	revCache *lru.TwoQueueCache[uint, string]

	imageCacheDir    string
	imageCacheServer string
}

func (s *Server) imagesEnabled() bool {
	return s.imageCacheServer != "" || s.imageCacheDir != ""
}

type cursorRecord struct {
	ID  uint `gorm:"primarykey"`
	Val int
}

func (s *Server) loadCursor() (int, error) {
	var rec cursorRecord
	if err := s.db.Find(&rec, "id = 1").Error; err != nil {
		return 0, err
	}
	if rec.ID == 0 {
		if err := s.db.Create(&cursorRecord{ID: 1}).Error; err != nil {
			return 0, err
		}
	}

	return rec.Val, nil
}

func (s *Server) syncCursorRoutine() {
	for range time.Tick(time.Second * 5) {
		if err := s.flushCursor(); err != nil {
			slog.Error("failed to flush cursor", "err", err)
		}
	}
}

func (s *Server) flushCursor() error {
	s.seqLk.Lock()
	v := s.lastSeq
	s.seqLk.Unlock()

	if err := s.db.Model(cursorRecord{}).Where("id = 1").Update("val", v).Error; err != nil {
		return err
	}

	return nil
}

type MarketConfig struct {
	gorm.Model
	RepoScanDone bool
}

func (s *Server) maybePumpRepos(ctx context.Context) error {
	var cfg MarketConfig
	if err := s.db.Find(&cfg, "id = 1").Error; err != nil {
		return err
	}

	if cfg.ID == 0 {
		cfg.ID = 1
		if err := s.db.Create(&cfg).Error; err != nil {
			return err
		}
	}

	if cfg.RepoScanDone {
		return nil
	}

	xrpcc := &xrpc.Client{
		Host: "https://bsky.network",
	}

	var curs string
	for {
		resp, err := atproto.SyncListRepos(ctx, xrpcc, curs, 1000)
		if err != nil {
			return err
		}

		for _, r := range resp.Repos {
			_, err := s.store.GetOrCreateJob(ctx, r.Did, backfill.StateEnqueued)
			if err != nil {
				slog.Error("failed to create backfill job", "did", r.Did, "err", err)
				continue
			}
		}

		if resp.Cursor != nil && *resp.Cursor != "" {
			curs = *resp.Cursor
		} else {
			break
		}
	}

	if err := s.db.Model(MarketConfig{}).Where("id = 1").Update("repo_scan_done", true).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) startLiveTail(curs int) error {
	slog.Info("starting live tail")

	// Connect to the Relay websocket
	urlStr := fmt.Sprintf("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", curs)

	d := websocket.DefaultDialer
	con, _, err := d.Dial(urlStr, http.Header{
		"User-Agent": []string{"market/0.0.1"},
	})
	if err != nil {
		return fmt.Errorf("failed to connect to relay: %w", err)
	}

	s.con = con

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {
			ctx := context.Background()

			s.seqLk.Lock()
			s.lastSeq = evt.Seq
			s.seqLk.Unlock()

			if err := s.bf.HandleEvent(ctx, evt); err != nil {
				return fmt.Errorf("handle event (%s,%d): %w", evt.Repo, evt.Seq, err)
			}

			return nil
		},
		RepoHandle: func(handle *atproto.SyncSubscribeRepos_Handle) error {
			return nil
		},
		RepoInfo: func(info *atproto.SyncSubscribeRepos_Info) error {
			return nil
		},
		RepoTombstone: func(tomb *atproto.SyncSubscribeRepos_Tombstone) error {
			return nil
		},
		// TODO: all the other event types
		Error: func(errf *events.ErrorFrame) error {
			return fmt.Errorf("error frame: %s: %s", errf.Error, errf.Message)
		},
	}

	settings := autoscaling.DefaultAutoscaleSettings()
	settings.Concurrency = 10
	settings.MaxConcurrency = 100

	sched := autoscaling.NewScheduler(settings, con.RemoteAddr().String(), rsc.EventHandler)

	s.eventScheduler = sched
	s.streamFinished = make(chan struct{})

	return nil
}

func sleepForWorksize(np int) {
	switch {
	case np == 0:
		time.Sleep(time.Second * 5)
	case np < 100:
		time.Sleep(time.Second * 4)
	case np < 500:
		time.Sleep(time.Second * 3)
	case np < 1000:
		time.Sleep(time.Second)
	}
}

func (s *Server) runCountAggregator() {
	for {
		np, err := s.aggregateCounts()
		if err != nil {
			slog.Error("failed to aggregate counts", "err", err)
		}

		sleepForWorksize(np)
	}
}

func (s *Server) aggregateCounts() (int, error) {
	start := time.Now()
	tx := s.db.Begin()

	var tasks []PostCountsTask
	if err := tx.Raw("DELETE FROM post_counts_tasks RETURNING *").Scan(&tasks).Error; err != nil {
		return 0, err
	}

	slog.Info("processing post count tasks", "count", len(tasks))

	batch := make(map[uint]*PostCounts)
	for _, t := range tasks {
		pc, ok := batch[t.Post]
		if !ok {
			pc = &PostCounts{}
			batch[t.Post] = pc
		}

		switch t.Op {
		case "like":
			pc.Likes += t.Val
		case "reply":
			pc.Replies += t.Val
		case "repost":
			pc.Reposts += t.Val
		case "quote":
			pc.Quotes += t.Val
		case "thread":
			pc.ThreadSize += t.Val
		default:
			return 0, fmt.Errorf("unrecognized counts task type: %q", t.Op)
		}
	}

	for post, counts := range batch {
		upd := make(map[string]any)
		if counts.Likes != 0 {
			upd["likes"] = gorm.Expr("likes + ?", counts.Likes)
		}
		if counts.Replies != 0 {
			upd["replies"] = gorm.Expr("replies + ?", counts.Replies)
		}
		if counts.Reposts != 0 {
			upd["reposts"] = gorm.Expr("reposts + ?", counts.Reposts)
		}
		if counts.Quotes != 0 {
			upd["quotes"] = gorm.Expr("quotes + ?", counts.Quotes)
		}
		if counts.ThreadSize != 0 {
			upd["thread_size"] = gorm.Expr("thread_size + ?", counts.ThreadSize)
		}
		if err := tx.Table("post_counts").Where("post = ?", post).Updates(upd).Error; err != nil {
			return 0, err
		}
	}

	if err := tx.Commit().Error; err != nil {
		return 0, err
	}

	took := time.Since(start)
	slog.Info("processed count tasks", "count", len(tasks), "time", took, "rate", float64(len(tasks))/took.Seconds())
	return len(tasks), nil
}

func (s *Server) getOrCreateRepo(ctx context.Context, did string) (*Repo, error) {
	r, ok := s.repoCache.Get(did)
	if !ok {
		s.reposLk.Lock()

		r, ok = s.repoCache.Get(did)
		if !ok {
			r = &Repo{}
			r.Did = did
			s.repoCache.Add(did, r)
		}

		s.reposLk.Unlock()
	}

	r.lk.Lock()
	defer r.lk.Unlock()
	if r.setup {
		return r, nil
	}

	if err := s.db.Find(r, "did = ?", did).Error; err != nil {
		return nil, err
	}

	if r.ID != 0 {
		// found it!
		r.setup = true
		return r, nil
	}

	r.Did = did
	if err := s.db.Create(r).Error; err != nil {
		return nil, err
	}

	r.setup = true

	return r, nil
}

func (s *Server) getOrCreateList(ctx context.Context, uri string) (*List, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return nil, err
	}

	r, err := s.getOrCreateRepo(ctx, puri.Did)
	if err != nil {
		return nil, err
	}

	// TODO: needs upsert treatment when we actually find the list
	var list List
	if err := s.db.FirstOrCreate(&list, map[string]any{
		"author": r.ID,
		"rkey":   puri.Rkey,
	}).Error; err != nil {
		return nil, err
	}
	return &list, nil
}

type cachedPostInfo struct {
	ID     uint
	Author uint
}

func (s *Server) postIDForUri(ctx context.Context, uri string) (uint, error) {
	v, ok := s.postInfoCache.Get(uri)
	if ok {
		return v.ID, nil
	}

	// getPostByUri implicitly fills the cache
	p, err := s.getPostByUri(ctx, uri)
	if err != nil {
		return 0, err
	}

	return p.ID, nil
}

func (s *Server) postInfoForUri(ctx context.Context, uri string) (*cachedPostInfo, error) {
	v, ok := s.postInfoCache.Get(uri)
	if ok {
		return v, nil
	}

	// getPostByUri implicitly fills the cache
	p, err := s.getPostByUri(ctx, uri)
	if err != nil {
		return nil, err
	}

	return &cachedPostInfo{ID: p.ID, Author: p.Author}, nil
}

func (s *Server) getPostByUri(ctx context.Context, uri string) (*Post, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return nil, err
	}

	r, err := s.getOrCreateRepo(ctx, puri.Did)
	if err != nil {
		return nil, err
	}

	var post Post
	if err := s.db.Find(&post, "author = ? AND rkey = ?", r.ID, puri.Rkey).Error; err != nil {
		return nil, err
	}

	if post.ID == 0 {
		post.Rkey = puri.Rkey
		post.Author = r.ID
		post.NotFound = true

		if err := s.db.Session(&gorm.Session{
			Logger: logger.Default.LogMode(logger.Silent),
		}).Create(&post).Error; err != nil {
			if !errors.Is(err, gorm.ErrDuplicatedKey) {
				return nil, err
			}
			if err := s.db.Find(&post, "author = ? AND rkey = ?", r.ID, puri.Rkey).Error; err != nil {
				return nil, fmt.Errorf("got duplicate post and still couldnt find it: %w", err)
			}
		}
		if err := s.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&PostCounts{Post: post.ID}).Error; err != nil {
			return nil, err
		}
	}

	s.postInfoCache.Add(uri, &cachedPostInfo{
		ID:     post.ID,
		Author: post.Author,
	})

	return &post, nil
}

type Repo struct {
	ID        uint `gorm:"primarykey"`
	CreatedAt time.Time
	Did       string `gorm:"uniqueIndex"`
	Handle    string

	// cache fields
	lk    sync.Mutex
	setup bool
}

type Post struct {
	ID        uint `gorm:"primarykey"`
	Created   time.Time
	Indexed   time.Time
	DeletedAt gorm.DeletedAt

	Author   uint   `gorm:"uniqueIndex:idx_post_rkeyauthor"`
	Rkey     string `gorm:"uniqueIndex:idx_post_rkeyauthor"`
	Raw      []byte
	NotFound bool
	Cid      string

	Reposting  uint
	ReplyTo    uint
	ReplyToUsr uint
	InThread   uint
}

type PostCounts struct {
	Post       uint `gorm:"uniqueindex"`
	Likes      int
	Replies    int
	Reposts    int
	Quotes     int
	ThreadSize int
}

type PostCountsTask struct {
	Post uint
	Op   string
	Val  int
}

type Follow struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Rkey    string `gorm:"uniqueIndex:idx_follow_rkeyauthor"`
	Author  uint   `gorm:"uniqueIndex:idx_follow_rkeyauthor"`
	Subject uint
}

type Block struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Rkey    string `gorm:"uniqueIndex:idx_blocks_rkeyauthor"`
	Author  uint   `gorm:"uniqueIndex:idx_blocks_rkeyauthor"`
	Subject uint
}

type Like struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_likes_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_likes_rkeyauthor"`
	Subject uint
}

type Repost struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_reposts_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_reposts_rkeyauthor"`
	Subject uint
}

type List struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_lists_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_lists_rkeyauthor"`
	Raw     []byte
}

type ListItem struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_listitems_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_listitems_rkeyauthor"`
	Subject uint
	List    uint
}

type ListBlock struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_listblocks_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_listblocks_rkeyauthor"`
	List    uint
}

type Profile struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Repo    uint
	Raw     []byte
}

type FeedGenerator struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_feedgen_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_feedgen_rkeyauthor"`
	Raw     []byte
	Did     string
}

type ThreadGate struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_threadgate_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_threadgate_rkeyauthor"`
	Raw     []byte
	Post    uint
}

type Image struct {
	ID     uint `gorm:"primarykey"`
	Post   uint `gorm:"index"`
	Cid    string
	Did    string
	Mime   string
	Cached bool
	Failed bool
}

func (s *Server) revForRepo(rr *Repo) (string, error) {
	lrev, ok := s.revCache.Get(rr.ID)
	if ok {
		return lrev, nil
	}

	var rev string
	if err := s.db.Raw("SELECT rev FROM gorm_db_jobs WHERE repo = ?", rr.Did).Scan(&rev).Error; err != nil {
		return "", err
	}

	if rev != "" {
		s.revCache.Add(rr.ID, rev)
	}
	return rev, nil
}

func (s *Server) handleCreate(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error {
	start := time.Now()

	rr, err := s.getOrCreateRepo(ctx, repo)
	if err != nil {
		return fmt.Errorf("get user failed: %w", err)
	}

	lrev, err := s.revForRepo(rr)
	if err != nil {
		return err
	}
	if lrev != "" {
		if rev < lrev {
			//slog.Info("skipping old rev create", "did", rr.Did, "rev", rev, "oldrev", lrev, "path", path)
			return nil
		}
	}

	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid path in handleCreate: %q", path)
	}
	col := parts[0]
	rkey := parts[1]

	defer func() {
		handleOpHist.WithLabelValues("create", col).Observe(float64(time.Since(start).Milliseconds()))
	}()

	if rkey == "" {
		fmt.Printf("messed up path: %q\n", rkey)
	}

	switch col {
	case "app.bsky.feed.post":
		if err := s.handleCreatePost(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.like":
		if err := s.handleCreateLike(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.repost":
		if err := s.handleCreateRepost(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.follow":
		if err := s.handleCreateFollow(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.block":
		if err := s.handleCreateBlock(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.list":
		if err := s.handleCreateList(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.listitem":
		if err := s.handleCreateListitem(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.listblock":
		if err := s.handleCreateListblock(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.actor.profile":
		if err := s.handleCreateProfile(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.generator":
		if err := s.handleCreateFeedGenerator(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.threadgate":
		if err := s.handleCreateThreadgate(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "chat.bsky.actor.declaration":
		if err := s.handleCreateChatDeclaration(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized record type: %q", col)
	}

	s.revCache.Add(rr.ID, rev)
	return nil
}

func (s *Server) handleCreatePost(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedPost
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	p := Post{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Raw:     recb,
	}

	if rec.Reply != nil && rec.Reply.Parent != nil {
		if rec.Reply.Root == nil {
			return fmt.Errorf("post reply had nil root")
		}

		pinfo, err := s.postInfoForUri(ctx, rec.Reply.Parent.Uri)
		if err != nil {
			return fmt.Errorf("getting reply parent: %w", err)
		}

		p.ReplyTo = pinfo.ID
		p.ReplyToUsr = pinfo.Author

		if err := s.db.Create(&PostCountsTask{
			Post: pinfo.ID,
			Op:   "reply",
			Val:  1,
		}).Error; err != nil {
			return err
		}

		thread, err := s.postIDForUri(ctx, rec.Reply.Root.Uri)
		if err != nil {
			return fmt.Errorf("getting thread root: %w", err)
		}

		p.InThread = thread

		if err := s.db.Create(&PostCountsTask{
			Post: thread,
			Op:   "thread",
			Val:  1,
		}).Error; err != nil {
			return err
		}
	}

	var images []*Image
	if rec.Embed != nil {

		var rpref string
		if rec.Embed.EmbedRecord != nil && rec.Embed.EmbedRecord.Record != nil {
			rpref = rec.Embed.EmbedRecord.Record.Uri
		}
		if rec.Embed.EmbedRecordWithMedia != nil &&
			rec.Embed.EmbedRecordWithMedia.Record != nil &&
			rec.Embed.EmbedRecordWithMedia.Record.Record != nil {
			rpref = rec.Embed.EmbedRecordWithMedia.Record.Record.Uri
		}

		if rpref != "" && strings.Contains(rpref, "app.bsky.feed.post") {
			rp, err := s.postIDForUri(ctx, rpref)
			if err != nil {
				return fmt.Errorf("getting quote subject: %w", err)
			}

			p.Reposting = rp

			if err := s.db.Create(&PostCountsTask{
				Post: rp,
				Op:   "quote",
				Val:  1,
			}).Error; err != nil {
				return err
			}

		}

		if rec.Embed.EmbedImages != nil {
			for _, img := range rec.Embed.EmbedImages.Images {
				if img.Image == nil {
					slog.Error("image had nil blob", "author", repo.ID, "rkey", rkey)
					continue
				}
				images = append(images, &Image{
					Cid:  img.Image.Ref.String(),
					Did:  repo.Did,
					Mime: img.Image.MimeType,
				})
			}
		}
	}

	if err := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "author"}, {Name: "rkey"}},
		DoUpdates: clause.AssignmentColumns([]string{"cid", "not_found", "raw", "created", "indexed"}),
	}).Create(&p).Error; err != nil {
		return err
	}
	if err := s.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&PostCounts{Post: p.ID}).Error; err != nil {
		return err
	}

	if len(images) > 0 {
		for _, img := range images {
			img.Post = p.ID
		}
		if err := s.db.Create(images).Error; err != nil {
			return err
		}
	}

	uri := "at://" + repo.Did + "/app.bsky.feed.post/" + rkey
	s.postInfoCache.Add(uri, &cachedPostInfo{
		ID:     p.ID,
		Author: p.Author,
	})

	return nil
}

func (s *Server) handleCreateLike(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedLike
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	pid, err := s.postIDForUri(ctx, rec.Subject.Uri)
	if err != nil {
		return fmt.Errorf("getting like subject: %w", err)
	}

	if err := s.db.Exec(`INSERT INTO "likes" ("created","indexed","author","rkey","subject") VALUES (?,?,?,?,?)`, created.Time(), time.Now(), repo.ID, rkey, pid).Error; err != nil {
		return err
	}

	if err := s.db.Create(&PostCountsTask{
		Post: pid,
		Op:   "like",
		Val:  1,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleCreateRepost(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedRepost
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}
	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	pid, err := s.postIDForUri(ctx, rec.Subject.Uri)
	if err != nil {
		return fmt.Errorf("getting repost subject: %w", err)
	}

	if err := s.db.Create(&Repost{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Subject: pid,
	}).Error; err != nil {
		return err
	}

	if err := s.db.Create(&PostCountsTask{
		Post: pid,
		Op:   "repost",
		Val:  1,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleCreateFollow(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphFollow
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}
	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	subj, err := s.getOrCreateRepo(ctx, rec.Subject)
	if err != nil {
		return err
	}

	if err := s.db.Create(&Follow{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Subject: subj.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleCreateBlock(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphBlock
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}
	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	subj, err := s.getOrCreateRepo(ctx, rec.Subject)
	if err != nil {
		return err
	}

	if err := s.db.Create(&Block{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Subject: subj.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleCreateList(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphList
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}
	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	if err := s.db.Create(&List{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Raw:     recb,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleCreateListitem(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphListitem
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}
	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	subj, err := s.getOrCreateRepo(ctx, rec.Subject)
	if err != nil {
		return err
	}

	list, err := s.getOrCreateList(ctx, rec.List)
	if err != nil {
		return err
	}

	if err := s.db.Create(&ListItem{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Subject: subj.ID,
		List:    list.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleCreateListblock(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphListblock
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}
	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	list, err := s.getOrCreateList(ctx, rec.Subject)
	if err != nil {
		return err
	}

	if err := s.db.Create(&ListBlock{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		List:    list.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleCreateProfile(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	if err := s.db.Create(&Profile{
		//Created: created.Time(),
		Indexed: time.Now(),
		Repo:    repo.ID,
		Raw:     recb,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleCreateFeedGenerator(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedGenerator
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	if err := s.db.Create(&FeedGenerator{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Did:     rec.Did,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleCreateThreadgate(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedThreadgate
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	pid, err := s.postIDForUri(ctx, rec.Post)
	if err != nil {
		return err
	}

	if err := s.db.Create(&ThreadGate{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Post:    pid,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleCreateChatDeclaration(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	// TODO: maybe track these?
	return nil
}

func (s *Server) handleUpdate(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error {
	// TODO:
	return nil
}

func (s *Server) handleDelete(ctx context.Context, repo string, rev string, path string) error {
	start := time.Now()

	rr, err := s.getOrCreateRepo(ctx, repo)
	if err != nil {
		return fmt.Errorf("get user failed: %w", err)
	}

	lrev, ok := s.revCache.Get(rr.ID)
	if ok {
		if rev < lrev {
			//slog.Info("skipping old rev delete", "did", rr.Did, "rev", rev, "oldrev", lrev)
			return nil
		}
	}

	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid path in handleDelete: %q", path)
	}
	col := parts[0]
	rkey := parts[1]

	defer func() {
		handleOpHist.WithLabelValues("create", col).Observe(float64(time.Since(start).Milliseconds()))
	}()

	switch col {
	case "app.bsky.feed.post":
		if err := s.handleDeletePost(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.feed.like":
		if err := s.handleDeleteLike(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.feed.repost":
		if err := s.handleDeleteRepost(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.follow":
		if err := s.handleDeleteFollow(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.block":
		if err := s.handleDeleteBlock(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.list":
		if err := s.handleDeleteList(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.listitem":
		if err := s.handleDeleteListitem(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.listblock":
		if err := s.handleDeleteListblock(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.actor.profile":
		if err := s.handleDeleteProfile(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.feed.generator":
		if err := s.handleDeleteFeedGenerator(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.feed.threadgate":
		if err := s.handleDeleteThreadgate(ctx, rr, rkey); err != nil {
			return err
		}
	default:
		return fmt.Errorf("delete unrecognized record type: %q", col)
	}

	s.revCache.Add(rr.ID, rev)
	return nil
}

func (s *Server) handleDeletePost(ctx context.Context, repo *Repo, rkey string) error {
	var p Post
	if err := s.db.Find(&p, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if p.ID == 0 {
		return fmt.Errorf("delete of unknown post record: %s %s", repo.Did, rkey)
	}

	var rec bsky.FeedPost
	if err := rec.UnmarshalCBOR(bytes.NewReader(p.Raw)); err != nil {
		return err
	}

	if rec.Reply != nil && rec.Reply.Parent != nil {
		reptoid, err := s.postIDForUri(ctx, rec.Reply.Parent.Uri)
		if err != nil {
			return fmt.Errorf("getting reply parent: %w", err)
		}

		p.ReplyTo = reptoid

		if err := s.db.Create(&PostCountsTask{
			Post: reptoid,
			Op:   "reply",
			Val:  -1,
		}).Error; err != nil {
			return err
		}

		thread, err := s.postIDForUri(ctx, rec.Reply.Root.Uri)
		if err != nil {
			return fmt.Errorf("getting thread root: %w", err)
		}

		p.InThread = thread

		if err := s.db.Create(&PostCountsTask{
			Post: thread,
			Op:   "thread",
			Val:  -1,
		}).Error; err != nil {
			return err
		}
	}

	if rec.Embed != nil {
		var rpref string
		if rec.Embed.EmbedRecord != nil && rec.Embed.EmbedRecord.Record != nil {
			rpref = rec.Embed.EmbedRecord.Record.Uri
		}
		if rec.Embed.EmbedRecordWithMedia != nil &&
			rec.Embed.EmbedRecordWithMedia.Record != nil &&
			rec.Embed.EmbedRecordWithMedia.Record.Record != nil {
			rpref = rec.Embed.EmbedRecordWithMedia.Record.Record.Uri
		}

		if rpref != "" && strings.Contains(rpref, "app.bsky.feed.post") {
			rp, err := s.postIDForUri(ctx, rpref)
			if err != nil {
				return fmt.Errorf("getting quote subject: %w", err)
			}

			p.Reposting = rp

			if err := s.db.Create(&PostCountsTask{
				Post: rp,
				Op:   "quote",
				Val:  -1,
			}).Error; err != nil {
				return err
			}

		}
	}

	if err := s.db.Delete(&Post{}, p.ID).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDeleteLike(ctx context.Context, repo *Repo, rkey string) error {
	var like Like
	if err := s.db.Find(&like, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if like.ID == 0 {
		return fmt.Errorf("delete of missing like: %s %s", repo.Did, rkey)
	}

	if err := s.db.Exec("DELETE FROM likes WHERE id = ?", like.ID).Error; err != nil {
		return err
	}

	if err := s.db.Create(&PostCountsTask{
		Post: like.Subject,
		Op:   "like",
		Val:  -1,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDeleteRepost(ctx context.Context, repo *Repo, rkey string) error {
	var repost Repost
	if err := s.db.Find(&repost, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if repost.ID == 0 {
		return fmt.Errorf("delete of missing repost: %s %s", repo.Did, rkey)
	}

	if err := s.db.Exec("DELETE FROM reposts WHERE id = ?", repost.ID).Error; err != nil {
		return err
	}

	if err := s.db.Create(&PostCountsTask{
		Post: repost.Subject,
		Op:   "repost",
		Val:  -1,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDeleteFollow(ctx context.Context, repo *Repo, rkey string) error {
	var follow Follow
	if err := s.db.Find(&follow, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if follow.ID == 0 {
		return fmt.Errorf("delete of missing follow: %s %s", repo.Did, rkey)
	}

	if err := s.db.Exec("DELETE FROM follows WHERE id = ?", follow.ID).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDeleteBlock(ctx context.Context, repo *Repo, rkey string) error {
	var block Block
	if err := s.db.Find(&block, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if block.ID == 0 {
		return fmt.Errorf("delete of missing block: %s %s", repo.Did, rkey)
	}

	if err := s.db.Exec("DELETE FROM blocks WHERE id = ?", block.ID).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDeleteList(ctx context.Context, repo *Repo, rkey string) error {
	var list List
	if err := s.db.Find(&list, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if list.ID == 0 {
		return fmt.Errorf("delete of missing list: %s %s", repo.Did, rkey)
	}

	if err := s.db.Exec("DELETE FROM lists WHERE id = ?", list.ID).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDeleteListitem(ctx context.Context, repo *Repo, rkey string) error {
	var item ListItem
	if err := s.db.Find(&item, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if item.ID == 0 {
		return fmt.Errorf("delete of missing listitem: %s %s", repo.Did, rkey)
	}

	if err := s.db.Exec("DELETE FROM list_items WHERE id = ?", item.ID).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDeleteListblock(ctx context.Context, repo *Repo, rkey string) error {
	var block ListBlock
	if err := s.db.Find(&block, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if block.ID == 0 {
		return fmt.Errorf("delete of missing listblock: %s %s", repo.Did, rkey)
	}

	if err := s.db.Exec("DELETE FROM list_blocks WHERE id = ?", block.ID).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDeleteFeedGenerator(ctx context.Context, repo *Repo, rkey string) error {
	var feedgen FeedGenerator
	if err := s.db.Find(&feedgen, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if feedgen.ID == 0 {
		return fmt.Errorf("delete of missing feedgen: %s %s", repo.Did, rkey)
	}

	if err := s.db.Exec("DELETE FROM feed_generators WHERE id = ?", feedgen.ID).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDeleteThreadgate(ctx context.Context, repo *Repo, rkey string) error {
	var threadgate ThreadGate
	if err := s.db.Find(&threadgate, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if threadgate.ID == 0 {
		return fmt.Errorf("delete of missing threadgate: %s %s", repo.Did, rkey)
	}

	if err := s.db.Exec("DELETE FROM thread_gates WHERE id = ?", threadgate.ID).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDeleteProfile(ctx context.Context, repo *Repo, rkey string) error {
	var profile Profile
	if err := s.db.Find(&profile, "repo = ?", repo.ID).Error; err != nil {
		return err
	}

	if profile.ID == 0 {
		return fmt.Errorf("delete of missing profile: %s %s", repo.Did, rkey)
	}

	if err := s.db.Exec("DELETE FROM profiles WHERE id = ?", profile.ID).Error; err != nil {
		return err
	}

	return nil
}

func (s *Server) imageFetcher() {
	for {
		var images []Image
		if err := s.db.Limit(1000).Order("id DESC").Find(&images, "NOT cached AND NOT failed").Error; err != nil {
			slog.Error("checking for images to cache failed", "err", err)
			time.Sleep(time.Second)
			continue
		}

		var uris []string
		for _, img := range images {
			uri, err := s.uriForImage(img.Did, img.Cid)
			if err != nil {
				slog.Error("failed to get uri for image", "err", err)
			}
			uris = append(uris, uri)
		}

		success := s.batchCacheImages(s.imageCacheDir, uris)

		if err := s.db.Transaction(func(tx *gorm.DB) error {
			for i := range images {
				if success[i] {
					if err := tx.Model(Image{}).Where("id = ?", images[i].ID).Update("cached", true).Error; err != nil {
						return err
					}
				} else {
					if err := tx.Model(Image{}).Where("id = ?", images[i].ID).Update("failed", true).Error; err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			slog.Error("failed to update database after image caching", "err", err)
		}

		if len(images) < 50 {
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (s *Server) getPostAuthorDid(p uint) (string, error) {
	var did string
	if err := s.db.Raw("SELECT did FROM posts LEFT JOIN repos ON repos.id = posts.author WHERE posts.id = ?", p).Scan(&did).Error; err != nil {
		return "", err
	}

	if did == "" {
		return "", fmt.Errorf("no repo found for post %d", p)
	}

	return did, nil
}

func (s *Server) uriForImage(did, cid string) (string, error) {
	return fmt.Sprintf("https://cdn.bsky.app/img/feed_fullsize/plain/%s/%s@jpeg", did, cid), nil
}

func (s *Server) batchCacheImages(dir string, images []string) []bool {
	n := 30
	sema := make(chan bool, n)

	results := make([]bool, len(images))
	for i, img := range images {
		sema <- true
		go func(i int, uri string) {
			defer func() {
				<-sema
			}()
			if err := s.maybeFetchImage(uri, dir); err != nil {
				fmt.Printf("image fetch failed (%s): %s\n", uri, err)
			} else {
				results[i] = true
			}
		}(i, img)
	}

	for i := 0; i < n; i++ {
		sema <- true
	}

	return results
}

func (s *Server) maybeFetchImage(uri string, dir string) error {
	parts := strings.Split(uri, "/")
	endbit := parts[len(parts)-1]
	cidpart := strings.Split(endbit, "@")[0]

	cached, err := s.imageIsCached(cidpart)
	if err != nil {
		return err
	}

	if cached {
		return nil
	}

	start := time.Now()
	var reqdo, tcopy, twrfile time.Time
	defer func() {
		fmt.Println("image fetch took: ", time.Since(start), reqdo.Sub(start), tcopy.Sub(start), twrfile.Sub(start))
	}()

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return err
	}

	if rlbypass := os.Getenv("BSKY_RATELIMITBYPASS"); rlbypass != "" {
		req.Header.Set("x-ratelimit-bypass", rlbypass)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch error: %w", err)
	}

	reqdo = time.Now()

	if resp.StatusCode != 200 {
		return fmt.Errorf("non-200 response code: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	tcopy = time.Now()

	if err := s.putImageToCache(cidpart, data); err != nil {
		return err
	}
	twrfile = time.Now()

	return nil
}

func (s *Server) imageIsCached(cc string) (bool, error) {
	if s.imageCacheServer != "" {
		resp, err := http.Head(s.imageCacheServer + "/" + cc)
		if err != nil {
			return false, err
		}

		switch resp.StatusCode {
		case 200:
			return true, nil
		case 404:
			return false, nil
		default:
			return false, fmt.Errorf("unrecognized status code: %d", resp.StatusCode)
		}
	} else {
		fp := filepath.Join(s.imageCacheDir, cc)
		_, err := os.Stat(fp)
		if err == nil {
			return true, nil
		}

		return false, nil
	}
}

func (s *Server) putImageToCache(cc string, b []byte) error {
	if s.imageCacheServer != "" {
		resp, err := http.Post(s.imageCacheServer+"/"+cc, "application/octet-stream", bytes.NewReader(b))
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("invalid response for post file: %d", resp.StatusCode)
		}

		return nil
	} else {
		fi, err := os.CreateTemp(s.imageCacheDir, "tempfi-*")
		if err != nil {
			return err
		}
		if _, err = fi.Write(b); err != nil {
			return err
		}
		fi.Close()

		fp := filepath.Join(s.imageCacheDir, cc)
		if err := os.Rename(fi.Name(), fp); err != nil {
			return fmt.Errorf("rename failed: %w", err)
		}

		return nil
	}
}

func (s *Server) getImageFromCache(did, cc string, w io.Writer, doresize bool) error {
	if s.imageCacheServer != "" {
		resp, err := http.Get(s.imageCacheServer + "/" + cc)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return fmt.Errorf("invalid status while reading file from cache: %d", resp.StatusCode)
		}

		if doresize {
			img, _, err := image.Decode(resp.Body)
			if err != nil {
				return err
			}

			nimg := resize.Resize(224, 224, img, resize.Lanczos3)

			return jpeg.Encode(w, nimg, nil)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		_, err = w.Write(body)
		return err
	} else if s.imageCacheDir != "" {
		p := filepath.Join(s.imageCacheDir, cc)
		fi, err := os.Open(p)
		if err != nil {
			return fmt.Errorf("failed to open image after fetching: %w", err)
		}
		defer fi.Close()

		if doresize {
			img, _, err := image.Decode(fi)
			if err != nil {
				return err
			}

			nimg := resize.Resize(224, 224, img, resize.Lanczos3)

			return jpeg.Encode(w, nimg, nil)
		}

		if _, err := io.Copy(w, fi); err != nil {
			return fmt.Errorf("COPY FAILED: %w", err)
		}
		return nil
	} else {
		return fmt.Errorf("image caching not enabled")
	}
}

func (s *Server) handleServeImage(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	did := parts[len(parts)-2]
	cc := parts[len(parts)-1]

	var doresize bool
	if r.URL.Query().Get("resize") != "" {
		doresize = true
	}

	fmt.Printf("SERVING IMAGE: %q %q (resize=%v)\n", did, cc, doresize)

	var img Image
	if err := s.db.Find(&img, "cid = ? AND did = ?", cc, did).Error; err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	uri, err := s.uriForImage(did, cc)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if err := s.maybeFetchImage(uri, s.imageCacheDir); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Add("Content-Type", img.Mime)

	if err := s.getImageFromCache(did, cc, w, doresize); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func (s *Server) crawlOldPostsForPictures() error {
	var oldestImage Image
	if err := s.db.Raw(`SELECT * FROM images ORDER BY post ASC LIMIT 1`).Scan(&oldestImage).Error; err != nil {
		return fmt.Errorf("failed to find oldest image: %w", err)
	}

	if oldestImage.ID == 0 {
		return nil
	}

	maxpost := oldestImage.Post

	for maxpost > 0 {
		var postsToCheck []Post
		if err := s.db.Raw(`SELECT * FROM posts WHERE id < ? AND NOT not_found ORDER BY id DESC LIMIT 200`, maxpost).Scan(&postsToCheck).Error; err != nil {
			return fmt.Errorf("getting more posts to check: %w", err)
		}
		if len(postsToCheck) == 0 {
			time.Sleep(time.Second * 10)
			continue
		}

		for _, p := range postsToCheck {
			if err := s.indexImagesInPost(&p); err != nil {
				slog.Error("failed to index post images", "post", p.ID, "err", err)
				continue
			}
		}
		maxpost = postsToCheck[len(postsToCheck)-1].ID
	}
	return nil
}

func (s *Server) indexImagesInPost(p *Post) error {
	var fp bsky.FeedPost

	if err := fp.UnmarshalCBOR(bytes.NewReader(p.Raw)); err != nil {
		return err
	}

	if fp.Embed == nil || fp.Embed.EmbedImages == nil {
		return nil
	}

	var did string
	if err := s.db.Raw("SELECT did FROM repos WHERE id = ?", p.Author).Scan(&did).Error; err != nil {
		return err
	}

	var images []Image
	for _, img := range fp.Embed.EmbedImages.Images {
		if img.Image == nil {
			slog.Error("image had nil blob", "author", p.Author, "rkey", p.Rkey)
			continue
		}
		images = append(images, Image{
			Post: p.ID,
			Cid:  img.Image.Ref.String(),
			Did:  did,
			Mime: img.Image.MimeType,
		})
	}

	if len(images) > 0 {
		if err := s.db.Create(images).Error; err != nil {
			return err
		}
	}

	return nil
}
