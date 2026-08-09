package batch

import (
	"context"

	"gorm.io/gorm"
)

type Order interface {
	getContext() context.Context
	getChunkSize() int
	getMaxWorkers() int
	getFilePath() string
	getDB() *gorm.DB
	submitWorker(func()) bool
}

type orderImpl struct {
	ctx        context.Context
	chunkSize  int
	maxWorkers int
	filepath   string
	db         *gorm.DB
	workers    chan struct{}
}

func (o *orderImpl) getContext() context.Context {
	return o.ctx
}

func (o *orderImpl) getChunkSize() int {
	return o.chunkSize
}

func (o *orderImpl) getMaxWorkers() int {
	return o.maxWorkers
}

func (o *orderImpl) getFilePath() string {
	return o.filepath
}

func (o *orderImpl) getDB() *gorm.DB {
	return o.db.Session(&gorm.Session{})
}

func (o *orderImpl) submitWorker(work func()) bool {
	if err := o.ctx.Err(); err != nil {
		return false
	}
	select {
	case <-o.ctx.Done():
		return false
	case o.workers <- struct{}{}:
	}
	go func() {
		defer func() { <-o.workers }()
		work()
	}()
	return true
}

func NewOrder(ctx context.Context, chunkSize, maxWorkers int, filepath string, db *gorm.DB) Order {
	if maxWorkers <= 0 {
		panic("maxWorkers must be a positive integer")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &orderImpl{
		ctx:        ctx,
		chunkSize:  chunkSize,
		maxWorkers: maxWorkers,
		filepath:   filepath,
		db:         db,
		workers:    make(chan struct{}, maxWorkers),
	}
}
