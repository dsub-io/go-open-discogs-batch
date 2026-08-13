package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/reactivex/rxgo/v2"
)

type sourceTransformer[F, T any] func(*F, time.Time) *T

func InsertSimple[F, T any](
	order Order,
	topic string,
	localName string,
	transform sourceTransformer[F, T],
) result.Result {
	r, err := newReadCloser(order.getFilePath(), fmt.Sprintf("source-read %+v", topic))
	if err != nil {
		return result.NewResult(0, err)
	}
	res := <-reader.NewReader[F](order.getContext(), r, localName).
		WindowWithCount(order.getChunkSize()).
		Map(helper.MapWindowedSlice[*F]()).
		Map(transformSourceChunk(transform)).
		Map(insertBySlice[*T](order), rxgo.WithPool(order.getMaxWorkers())).
		Reduce(helper.MergeCount()).
		Observe()
	return simpleInsertResult(topic, res)
}

func transformSourceChunk[F, T any](
	transform sourceTransformer[F, T],
) func(context.Context, interface{}) (interface{}, error) {
	return func(ctx context.Context, value interface{}) (interface{}, error) {
		sources := value.([]*F)
		observedAt := time.Now().UTC()
		rows := make([]*T, 0, len(sources))
		for _, source := range sources {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if source == nil {
				continue
			}
			row := transform(source, observedAt)
			if row == nil {
				continue
			}
			_, err := registerCache(ctx, row)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
		return rows, nil
	}
}

func simpleInsertResult(topic string, res rxgo.Item) result.Result {
	if res.E != nil {
		return result.NewResult(0, res.E)
	}
	updated, ok := res.V.(int)
	if !ok {
		return result.NewResult(0, fmt.Errorf("insert result for %s did not contain a count", topic))
	}
	fmt.Printf("\nUpdated %+v %+v\n", updated, topic)
	return result.NewResult(updated, nil)
}

func insertBySlice[T any](order Order) func(_ context.Context, i interface{}) (interface{}, error) {
	return func(_ context.Context, i interface{}) (interface{}, error) {
		res := executeActiveRunTransaction(order, func(transactionOrder Order) result.Result {
			return NewWriter(transactionOrder.getDB()).Write(
				transactionOrder.getChunkSize(),
				i.([]T),
			)
		})
		return res.Count(), res.Err()
	}
}

func registerCache(_ context.Context, i interface{}) (interface{}, error) {
	if i == nil {
		return i, nil
	}
	switch o := i.(type) {
	case *model.Artist:
		cache.ArtistIDs.Add(o.ID)
	case *model.Label:
		cache.LabelIDs.Add(o.ID)
	case *model.Master:
		cache.MasterIDs.Add(o.ID)
	}
	return i, nil
}
