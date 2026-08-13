package batch

import "time"

type observedAtTarget interface {
	setObservedAt(time.Time)
}

func assignChunkObservedAt[T observedAtTarget](items []T, observedAt time.Time) {
	for _, item := range items {
		item.setObservedAt(observedAt)
	}
}
