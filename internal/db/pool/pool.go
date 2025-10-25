package pool

import (
	"context"
	"log/slog"
	"sync"
)

const (
	DefaultWorkerCount = 10
	BufferMultiplier   = 2
)

type Fn func(ctx context.Context) error

type Pool struct {
	workers  int
	jobQueue chan Fn
	logger   *slog.Logger
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	running  bool
	mu       sync.RWMutex
}

type Builder struct {
	workers int
	logger  *slog.Logger
}

func NewBuilder() *Builder {
	return &Builder{
		workers: DefaultWorkerCount,
		logger:  slog.Default(),
	}
}

func (b *Builder) WithWorkers(workers int) *Builder {
	if workers <= 0 {
		workers = 1
	}
	b.workers = workers
	return b
}

func (b *Builder) WithLogger(logger *slog.Logger) *Builder {
	b.logger = logger
	return b
}

func (b *Builder) Build(ctx context.Context) *Pool {
	ctx, cancel := context.WithCancel(ctx)
	pool := &Pool{
		workers:  b.workers,
		jobQueue: make(chan Fn, b.workers*BufferMultiplier),
		logger:   b.logger.WithGroup("pool"),
		ctx:      ctx,
		cancel:   cancel,
		running:  true,
	}

	pool.start()
	return pool
}

func (p *Pool) start() {
	p.wg.Add(p.workers)
	for i := 0; i < p.workers; i++ {
		go p.worker(i)
	}
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()

	logger := p.logger.With("worker", id)
	logger.Debug("worker started")

	for {
		select {
		case <-p.ctx.Done():
			logger.Debug("worker shutting down")
			return
		case job := <-p.jobQueue:
			p.executeJob(logger, job)
		}
	}
}

func (p *Pool) executeJob(logger *slog.Logger, job Fn) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("job panicked", "panic", r)
		}
	}()

	if err := job(p.ctx); err != nil {
		logger.Error("job returned error", "error", err)
	}
}

func (p *Pool) Submit(job Fn) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if !p.running {
		p.logger.Error("attempted to submit job to stopped pool")
		return
	}

	select {
	case p.jobQueue <- job:
	case <-p.ctx.Done():
		p.logger.Error("attempted to submit job to stopped pool")
	}
}

func (p *Pool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return
	}

	p.running = false
	p.cancel()
	close(p.jobQueue)
	p.wg.Wait()
	p.logger.Info("pool stopped", "workers", p.workers)
}

// Wait blocks until all submitted jobs have been processed
func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}

func (p *Pool) Workers() int {
	return p.workers
}
