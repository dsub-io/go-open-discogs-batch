package batch

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrderLimitsConcurrentWorkers(t *testing.T) {
	const maxWorkers = 2
	const totalWorkers = 8
	order := NewOrder(context.Background(), 1, maxWorkers, "unused", nil)
	var active atomic.Int32
	var peak atomic.Int32
	started := make(chan struct{}, totalWorkers)
	release := make(chan struct{})
	results := make(chan bool, totalWorkers)
	var submitters sync.WaitGroup
	var workers sync.WaitGroup

	for range totalWorkers {
		submitters.Add(1)
		workers.Add(1)
		go func() {
			defer submitters.Done()
			results <- order.submitWorker(context.Background(), func() {
				defer workers.Done()
				current := active.Add(1)
				for {
					previous := peak.Load()
					if current <= previous || peak.CompareAndSwap(previous, current) {
						break
					}
				}
				started <- struct{}{}
				<-release
				active.Add(-1)
			})
		}()
	}
	for range maxWorkers {
		<-started
	}
	require.Equal(t, int32(maxWorkers), peak.Load())
	close(release)
	submitters.Wait()
	workers.Wait()
	close(results)
	for submitted := range results {
		require.True(t, submitted)
	}

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

	require.False(t, order.submitWorker(ctx, func() {}))
}
