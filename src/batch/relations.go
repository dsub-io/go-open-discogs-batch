package batch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
)

type relationChunkWriter[T any] func(Order, ChunkMetadata, []*T) result.Result
type relationFinalizer func(Order, int64, int64) result.Result

func reconcileRelations(reconcile []func() result.Result) result.Result {
	written := result.NewResult(0, nil)
	for _, reconcileRelation := range reconcile {
		written = written.Sum(reconcileRelation())
		if written.IsErr() {
			return written
		}
	}
	return written
}

type sourceErrorRecorder struct {
	mu  sync.Mutex
	err error
}

func (r *sourceErrorRecorder) record(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	r.err = err
	fmt.Printf("[ERROR] %+v %+v\n", time.Now().Format(time.Layout), err)
}

func (r *sourceErrorRecorder) get() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func processRelationChunks[T any](
	order Order,
	topic string,
	localName string,
	progressText string,
	writeRelationChunk relationChunkWriter[T],
) result.Result {
	return processRelationChunksWithFinalizer(
		order,
		topic,
		localName,
		progressText,
		writeRelationChunk,
		completeRelationImport,
	)
}

func processRelationChunksWithFinalizer[T any](
	order Order,
	topic string,
	localName string,
	progressText string,
	writeRelationChunk relationChunkWriter[T],
	finalize relationFinalizer,
) result.Result {
	reporter := newEntityProgressReporter(order)
	reporter.Start()
	completedChunks, completedChunksError := loadCompletedChunkInventory(order)
	if completedChunksError != nil {
		reporter.Finish(false)
		return result.NewResult(0, completedChunksError)
	}
	ctx, cancel := context.WithCancel(order.getContext())
	defer cancel()
	results := make(chan result.Result)
	var workers sync.WaitGroup
	sourceErrors := new(sourceErrorRecorder)
	var totalItems int64
	var totalChunks int64
	source, err := newReadCloser(order.getFilePath(), progressText)
	if err != nil {
		reporter.Finish(false)
		return result.NewResult(0, err)
	}

	reader.NewReader[T](
		ctx,
		source,
		localName,
	).
		WindowWithCount(order.getChunkSize()).
		Map(helper.MapWindowedSlice[*T]()).
		ForEach(
			func(value interface{}) {
				items := value.([]*T)
				if len(items) == 0 {
					return
				}
				chunk := ChunkMetadata{
					Index:          totalChunks,
					FirstItemIndex: totalItems,
					ItemCount:      len(items),
				}
				totalChunks++
				totalItems += int64(len(items))
				completed, completedError := completedChunks.contains(chunk)
				if completedError != nil {
					sourceErrors.record(completedError)
					cancel()
					return
				}
				if completed {
					return
				}
				workers.Add(1)
				if order.submitWorker(ctx, func() {
					defer workers.Done()
					written := writeRelationChunk(order, chunk, items)
					reporter.Observe()
					if written.IsErr() {
						cancel()
					}
					results <- written
				}) {
					return
				}
				workers.Done()
				sourceErrors.record(ctx.Err())
			},
			sourceErrors.record,
			func() {
				workers.Wait()
				close(results)
			},
		)

	sum := result.NewResult(0, nil)
	for next := range results {
		sum = sum.Sum(next)
	}
	sum = mergeSourceError(sum, sourceErrors.get())
	if !sum.IsErr() {
		sum = sum.Sum(finalize(order, totalItems, totalChunks))
	}
	reporter.Finish(!sum.IsErr())

	fmt.Printf("\nUpdated %+v %s\n", sum.Count(), topic)
	return sum
}

func completeRelationImport(order Order, totalItems, totalChunks int64) result.Result {
	return result.NewResult(0, completeEntityProgress(order, totalItems, totalChunks))
}

func mergeSourceError(sum result.Result, sourceErr error) result.Result {
	if sourceErr == nil {
		return sum
	}
	return sum.Sum(result.NewResult(0, sourceErr))
}
