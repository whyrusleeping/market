package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/backfill"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/gorm"
)

type Pgxjob struct {
	repo  string
	state string
	rev   string

	lk          sync.Mutex
	bufferedOps []*opSet

	jobID uint
	//dbj *GormDBJob
	db *pgxpool.Pool

	retryCount int
	retryAfter *time.Time
}

type opSet struct {
	since *string
	rev   string
	ops   []*backfill.BufferedOp
}

type pgxstorejob struct {
	gorm.Model
	Repo       string `gorm:"unique;index"`
	State      string `gorm:"index:enqueued_job_idx,where:state = 'enqueued';index:retryable_job_idx,where:state like 'failed%'"`
	Rev        string
	RetryCount int
	RetryAfter *time.Time `gorm:"index:retryable_job_idx,sort:desc"`
}

// Pgxstore is a gorm-backed implementation of the Backfill Store interface
type Pgxstore struct {
	lk   sync.RWMutex
	jobs map[string]*Pgxjob

	qlk       sync.Mutex
	taskQueue []string

	db *pgxpool.Pool
}

func NewPgxStore(pool *pgxpool.Pool) (*Pgxstore, error) {
	/*
		_, err := pool.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS gorm_db_jobs (
			id BIGSERIAL PRIMARY KEY,
			created_at TIMESTAMP WITH TIME ZONE,
			updated_at TIMESTAMP WITH TIME ZONE,
			deleted_at TIMESTAMP WITH TIME ZONE,
			repo TEXT,
			state TEXT,
			rev TEXT,
			retry_count BIGINT,
			retry_after TIMESTAMP WITH TIME ZONE
		);

		-- Create indexes if they don't exist
		CREATE INDEX IF NOT EXISTS idx_gorm_db_jobs_deleted_at ON gorm_db_jobs (deleted_at);
		CREATE INDEX IF NOT EXISTS idx_gorm_db_jobs_repo ON gorm_db_jobs (repo);
		CREATE INDEX IF NOT EXISTS idx_gorm_db_jobs_state ON gorm_db_jobs (state);

		-- Create unique constraint if it doesn't exist
		DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint
				WHERE conname = 'gorm_db_jobs_repo_key'
				AND conrelid = 'gorm_db_jobs'::regclass
			) THEN
				ALTER TABLE gorm_db_jobs ADD CONSTRAINT gorm_db_jobs_repo_key UNIQUE (repo);
			END IF;
		END $$;

		-- Create conditional indexes
		CREATE INDEX IF NOT EXISTS enqueued_job_idx ON gorm_db_jobs (state) WHERE state = 'enqueued';
		CREATE INDEX IF NOT EXISTS retryable_job_idx ON gorm_db_jobs (state, retry_after DESC) WHERE state LIKE 'failed%';
		`)
		if err != nil {
			return nil, err
		}
	*/

	return &Pgxstore{
		jobs: make(map[string]*Pgxjob),
		db:   pool,
	}, nil
}

func (s *Pgxstore) LoadJobs(ctx context.Context) error {
	s.qlk.Lock()
	defer s.qlk.Unlock()
	return s.loadJobs(ctx, 20_000)
}

func (s *Pgxstore) getEnqueuedJobs(ctx context.Context, limit int) ([]string, error) {
	enqueuedSelect := `SELECT repo FROM gorm_db_jobs WHERE state  = 'enqueued' LIMIT $1`
	rows, err := s.db.Query(ctx, enqueuedSelect, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var todo []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		todo = append(todo, val)
	}
	return todo, nil
}

func (s *Pgxstore) loadJobs(ctx context.Context, limit int) error {
	todo, err := s.getEnqueuedJobs(ctx, limit)
	if err != nil {
		return err
	}

	if len(todo) < limit {
		retryableSelect := `SELECT repo FROM gorm_db_jobs WHERE state like 'failed%%' AND (retry_after = NULL OR retry_after < $1) LIMIT $2`
		rows, err := s.db.Query(ctx, retryableSelect, time.Now(), limit-len(todo))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var next string
			if err := rows.Scan(&next); err != nil {
				return err
			}
			todo = append(todo, next)
		}
	}

	s.taskQueue = append(s.taskQueue, todo...)

	return nil
}

func (s *Pgxstore) GetOrCreateJob(ctx context.Context, repo, state string) (backfill.Job, error) {
	j, err := s.getJob(ctx, repo)
	if err == nil {
		return j, nil
	}

	if !errors.Is(err, backfill.ErrJobNotFound) {
		return nil, err
	}

	if err := s.createJobForRepo(repo, state); err != nil {
		return nil, err
	}

	return s.getJob(ctx, repo)
}

func (s *Pgxstore) EnqueueJob(ctx context.Context, repo string) error {
	_, err := s.GetOrCreateJob(ctx, repo, backfill.StateEnqueued)
	if err != nil {
		return err
	}

	s.qlk.Lock()
	s.taskQueue = append(s.taskQueue, repo)
	s.qlk.Unlock()

	return nil
}

func (s *Pgxstore) EnqueueJobWithState(ctx context.Context, repo, state string) error {
	_, err := s.GetOrCreateJob(ctx, repo, state)
	if err != nil {
		return err
	}

	s.qlk.Lock()
	s.taskQueue = append(s.taskQueue, repo)
	s.qlk.Unlock()

	return nil
}

func (s *Pgxstore) createJobForRepo(repo, state string) error {
	/*
		dbj := &GormDBJob{
			Repo:  repo,
			State: state,
		}
	*/

	ctx := context.TODO()

	q := "INSERT INTO gorm_db_jobs (repo, state, created_at, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT (repo) DO NOTHING RETURNING id"

	var idout uint
	if err := s.db.QueryRow(ctx, q, repo, state, time.Now(), time.Now()).Scan(&idout); err != nil {
		return err
	}

	s.lk.Lock()
	defer s.lk.Unlock()

	if _, ok := s.jobs[repo]; ok {
		return fmt.Errorf("job already exists for repo %s", repo)
	}

	j := &Pgxjob{
		repo:  repo,
		state: state,

		jobID: idout,

		db: s.db,
	}
	s.jobs[repo] = j

	return nil
}

func (j *Pgxjob) BufferOps(ctx context.Context, since *string, rev string, ops []*backfill.BufferedOp) (bool, error) {
	j.lk.Lock()
	defer j.lk.Unlock()

	switch j.state {
	case backfill.StateComplete:
		return false, nil
	case backfill.StateInProgress, backfill.StateEnqueued:
		// keep going and buffer the op
	default:
		if strings.HasPrefix(j.state, "failed") {
			if j.retryCount >= backfill.MaxRetries {
				// Process immediately since we're out of retries
				return false, nil
			}
			// Don't buffer the op since it'll get caught in the next retry (hopefully)
			return true, nil
		}
		return false, fmt.Errorf("invalid job state: %q", j.state)
	}

	if j.rev >= rev || (since == nil && j.rev != "") {
		// we've already accounted for this event
		return false, backfill.ErrAlreadyProcessed
	}

	j.bufferOps(&opSet{since: since, rev: rev, ops: ops})
	return true, nil
}

func (j *Pgxjob) bufferOps(ops *opSet) {
	j.bufferedOps = append(j.bufferedOps, ops)
}

func (s *Pgxstore) GetJob(ctx context.Context, repo string) (backfill.Job, error) {
	return s.getJob(ctx, repo)
}

func (s *Pgxstore) getJob(ctx context.Context, repo string) (*Pgxjob, error) {
	cj := s.checkJobCache(ctx, repo)
	if cj != nil {
		return cj, nil
	}

	return s.loadJob(ctx, repo)
}

func maybeTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func (s *Pgxstore) loadJob(ctx context.Context, repo string) (*Pgxjob, error) {
	var jobid uint
	var state, rev *string
	var retryCount *int
	var createdAt, updatedAt, retryAt *time.Time

	if err := s.db.QueryRow(ctx, "SELECT id, created_at, updated_at, state, rev, retry_count, retry_after FROM gorm_db_jobs WHERE repo = $1", repo).Scan(&jobid, &createdAt, &updatedAt, &state, &rev, &retryCount, &retryAt); err != nil {
		if err == pgx.ErrNoRows {
			return nil, backfill.ErrJobNotFound
		}
		return nil, err
	}

	j := &Pgxjob{
		repo:       repo,
		retryAfter: retryAt,

		jobID: jobid,

		db: s.db,
	}
	if state != nil {
		j.state = *state
	}
	if rev != nil {
		j.rev = *rev
	}
	if retryCount != nil {
		j.retryCount = *retryCount
	}

	s.lk.Lock()
	defer s.lk.Unlock()
	// would imply a race condition
	exist, ok := s.jobs[repo]
	if ok {
		return exist, nil
	}
	s.jobs[repo] = j
	return j, nil
}

func (s *Pgxstore) checkJobCache(ctx context.Context, repo string) *Pgxjob {
	s.lk.RLock()
	defer s.lk.RUnlock()

	j, ok := s.jobs[repo]
	if !ok || j == nil {
		return nil
	}
	return j
}

func (s *Pgxstore) GetNextEnqueuedJob(ctx context.Context) (backfill.Job, error) {
	s.qlk.Lock()
	defer s.qlk.Unlock()
	if len(s.taskQueue) == 0 {
		if err := s.loadJobs(ctx, 1000); err != nil {
			return nil, err
		}

		if len(s.taskQueue) == 0 {
			return nil, nil
		}
	}

	for len(s.taskQueue) > 0 {
		first := s.taskQueue[0]
		s.taskQueue = s.taskQueue[1:]

		j, err := s.getJob(ctx, first)
		if err != nil {
			return nil, err
		}

		shouldRetry := strings.HasPrefix(j.State(), "failed") && j.retryAfter != nil && time.Now().After(*j.retryAfter)

		if j.State() == backfill.StateEnqueued || shouldRetry {
			return j, nil
		}
	}
	return nil, nil
}

func (j *Pgxjob) Repo() string {
	return j.repo
}

func (j *Pgxjob) State() string {
	j.lk.Lock()
	defer j.lk.Unlock()

	return j.state
}

func (j *Pgxjob) SetRev(ctx context.Context, r string) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	j.rev = r

	_, err := j.db.Exec(ctx, "UPDATE gorm_db_jobs SET rev = $1, updated_at = $2 WHERE id = $3", r, time.Now(), j.jobID)
	return err
}

func (j *Pgxjob) Rev() string {
	j.lk.Lock()
	defer j.lk.Unlock()

	return j.rev
}

func computeExponentialBackoff(attempt int) time.Duration {
	return time.Duration(1<<uint(attempt)) * 10 * time.Second
}

func (j *Pgxjob) SetState(ctx context.Context, state string) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	j.state = state

	if strings.HasPrefix(state, "failed") {
		if j.retryCount < backfill.MaxRetries {
			next := time.Now().Add(computeExponentialBackoff(j.retryCount))
			j.retryAfter = &next
			j.retryCount++
		} else {
			j.retryAfter = nil
		}
	}

	_, err := j.db.Exec(ctx, "UPDATE gorm_db_jobs SET state = $1, updated_at = $2 WHERE id = $3", state, time.Now(), j.jobID)
	return err
}

func (j *Pgxjob) FlushBufferedOps(ctx context.Context, fn func(kind repomgr.EventKind, rev, path string, rec *[]byte, cid *cid.Cid) error) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	for _, opset := range j.bufferedOps {
		if opset.rev <= j.rev {
			// stale events, skip
			continue
		}

		if opset.since == nil {
			// The first event for a repo may have a nil since
			// We should process it only if the rev is empty, skip otherwise
			if j.rev != "" {
				continue
			}
		}

		if j.rev > *opset.since {
			// we've already accounted for this event
			continue
		}

		if j.rev != *opset.since {
			// we've got a discontinuity
			return fmt.Errorf("event since did not match current rev (%s != %s): %w", *opset.since, j.rev, backfill.ErrEventGap)
		}

		for _, op := range opset.ops {
			if err := fn(op.Kind, opset.rev, op.Path, op.Record, op.Cid); err != nil {
				return err
			}
		}

		j.rev = opset.rev
	}

	j.bufferedOps = []*opSet{}
	j.state = backfill.StateComplete

	return nil
}

func (j *Pgxjob) ClearBufferedOps(ctx context.Context) error {
	j.lk.Lock()
	defer j.lk.Unlock()

	j.bufferedOps = []*opSet{}
	return nil
}

func (j *Pgxjob) RetryCount() int {
	j.lk.Lock()
	defer j.lk.Unlock()
	return j.retryCount
}

func (s *Pgxstore) UpdateRev(ctx context.Context, repo, rev string) error {
	j, err := s.GetJob(ctx, repo)
	if err != nil {
		return err
	}

	return j.SetRev(ctx, rev)
}

func (s *Pgxstore) PurgeRepo(ctx context.Context, repo string) error {
	if _, err := s.db.Exec(ctx, "DELETE FROM gorm_db_jobs WHERE repo = $1", repo); err != nil {
		return err
	}

	s.lk.Lock()
	defer s.lk.Unlock()
	delete(s.jobs, repo)

	return nil
}
