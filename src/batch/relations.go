package batch

import (
	"fmt"
	"sync"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
)

type relationChunkWriter[T any] func(Order, []*T) result.Result

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
	results := make(chan result.Result)
	var workers sync.WaitGroup
	sourceErrors := new(sourceErrorRecorder)

	reader.NewReader[T](
		order.getContext(),
		newReadCloser(order.getFilePath(), progressText),
		localName,
	).
		WindowWithCount(order.getChunkSize()).
		Map(helper.MapWindowedSlice[*T]()).
		ForEach(
			func(value interface{}) {
				items := value.([]*T)
				workers.Add(1)
				if order.submitWorker(func() {
					defer workers.Done()
					results <- writeRelationChunk(order, items)
				}) {
					return
				}
				workers.Done()
				sourceErrors.record(order.getContext().Err())
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
	if sourceErr := sourceErrors.get(); sourceErr != nil {
		sum = sum.Sum(result.NewResult(0, sourceErr))
	}

	fmt.Printf("\nUpdated %+v %s\n", sum.Count(), topic)
	return sum
}
