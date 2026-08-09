package batch

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOrderLimitsConcurrentWorkers(t *testing.T) {
	const maxWorkers = 2
	order := NewOrder(context.Background(), 1, maxWorkers, "unused", nil)
	var active atomic.Int32
	var peak atomic.Int32
	var workers sync.WaitGroup

	for range 8 {
		workers.Add(1)
		order.submitWorker(func() {
			defer workers.Done()
			current := active.Add(1)
			for {
				previous := peak.Load()
				if current <= previous || peak.CompareAndSwap(previous, current) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			active.Add(-1)
		})
	}
	workers.Wait()

	require.Equal(t, int32(maxWorkers), peak.Load())
	require.Equal(t, maxWorkers, order.getMaxWorkers())
}

func TestOrderRejectsNonPositiveMaxWorkers(t *testing.T) {
	require.PanicsWithValue(
		t,
		"maxWorkers must be a positive integer",
		func() { NewOrder(context.Background(), 1, 0, "unused", nil) },
	)
}

func TestOrderStopsSubmittingAfterContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	order := NewOrder(ctx, 1, 1, "unused", nil)

	require.False(t, order.submitWorker(func() {}))
}
