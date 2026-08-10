package batch

import (
	"context"

	"gorm.io/gorm"
)

type Order interface {
	getContext() context.Context
	getChunkSize() int
	getEntityType() string
	getMaxWorkers() int
	getFilePath() string
	getRunID() int64
	getDB() *gorm.DB
	submitWorker(context.Context, func()) bool
	withDB(*gorm.DB) Order
}

type orderImpl struct {
	ctx        context.Context
	chunkSize  int
	entityType string
	maxWorkers int
	filepath   string
	runID      int64
	db         *gorm.DB
	workers    chan struct{}
}

func (o *orderImpl) getContext() context.Context {
	return o.ctx
}

func (o *orderImpl) getChunkSize() int {
	return o.chunkSize
}

func (o *orderImpl) getEntityType() string {
	return o.entityType
}

func (o *orderImpl) getMaxWorkers() int {
	return o.maxWorkers
}

func (o *orderImpl) getFilePath() string {
	return o.filepath
}

func (o *orderImpl) getRunID() int64 {
	return o.runID
}

func (o *orderImpl) getDB() *gorm.DB {
	return o.db.Session(&gorm.Session{})
}

func (o *orderImpl) submitWorker(ctx context.Context, work func()) bool {
	if ctx == nil {
		ctx = o.ctx
	}
	if err := ctx.Err(); err != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case o.workers <- struct{}{}:
	}
	go func() {
		defer func() { <-o.workers }()
		work()
	}()
	return true
}

func (o *orderImpl) withDB(db *gorm.DB) Order {
	copy := *o
	copy.db = db
	return &copy
}

func NewOrder(ctx context.Context, chunkSize, maxWorkers int, filepath string, db *gorm.DB) Order {
	return newOrder(ctx, chunkSize, maxWorkers, filepath, db, 0, "")
}

func NewTrackedOrder(
	ctx context.Context,
	chunkSize int,
	maxWorkers int,
	filepath string,
	db *gorm.DB,
	runID int64,
	entityType string,
) Order {
	if runID <= 0 {
		panic("runID must be a positive integer")
	}
	if entityType == "" {
		panic("entityType must not be empty")
	}
	return newOrder(ctx, chunkSize, maxWorkers, filepath, db, runID, entityType)
}

func newOrder(
	ctx context.Context,
	chunkSize int,
	maxWorkers int,
	filepath string,
	db *gorm.DB,
	runID int64,
	entityType string,
) Order {
	if chunkSize <= 0 {
		panic("chunkSize must be a positive integer")
	}
	if maxWorkers <= 0 {
		panic("maxWorkers must be a positive integer")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &orderImpl{
		ctx:        ctx,
		chunkSize:  chunkSize,
		entityType: entityType,
		maxWorkers: maxWorkers,
		filepath:   filepath,
		runID:      runID,
		db:         db,
		workers:    make(chan struct{}, maxWorkers),
	}
}
