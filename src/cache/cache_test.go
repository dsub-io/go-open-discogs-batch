package cache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIDSetStoresPositiveIDsAcrossSegments(t *testing.T) {
	ids := new(IDSet)

	for _, id := range []int32{1, 65_535, 65_536, 200_000_000} {
		ids.Add(id)
	}

	for _, id := range []int32{1, 65_535, 65_536, 200_000_000} {
		require.True(t, ids.Contains(id))
	}
	require.False(t, ids.Contains(2))
	require.Equal(t, int64(3*8_192), ids.AllocatedWordBytes())
}

func TestIDSetSupportsConcurrentRegistration(t *testing.T) {
	ids := new(IDSet)
	var workers sync.WaitGroup
	for worker := int32(0); worker < 8; worker++ {
		workers.Add(1)
		go func(offset int32) {
			defer workers.Done()
			for id := offset + 1; id <= 100_000; id += 8 {
				ids.Add(id)
			}
		}(worker)
	}
	workers.Wait()

	require.True(t, ids.Contains(1))
	require.True(t, ids.Contains(50_000))
	require.True(t, ids.Contains(100_000))
	require.False(t, ids.Contains(100_001))
}

func TestIDSetIgnoresInvalidIDsAndResets(t *testing.T) {
	ids := new(IDSet)
	ids.Add(-1)
	ids.Add(0)
	ids.Add(42)
	ids.Add(42)

	require.False(t, ids.Contains(-1))
	require.True(t, ids.Contains(42))
	require.Equal(t, int64(8_192), ids.AllocatedWordBytes())

	ids.Reset()
	require.False(t, ids.Contains(42))
	require.Zero(t, ids.AllocatedWordBytes())
}

func TestResetIDsClearsGlobalIdentifierCaches(t *testing.T) {
	ArtistIDs.Add(1)
	LabelIDs.Add(2)
	MasterIDs.Add(3)
	GenreNames.Add("Electronic")
	StyleNames.Add("Techno")

	ResetIDs()

	require.False(t, ArtistIDs.Contains(1))
	require.False(t, LabelIDs.Contains(2))
	require.False(t, MasterIDs.Contains(3))
	require.False(t, GenreNames.Contains("Electronic"))
	require.False(t, StyleNames.Contains("Techno"))
}

func TestNameSetSupportsConcurrentConfirmation(t *testing.T) {
	names := new(NameSet)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			names.Add("Electronic")
		}()
	}
	workers.Wait()

	require.True(t, names.Contains("Electronic"))
	require.False(t, names.Contains("Techno"))
	names.Reset()
	require.False(t, names.Contains("Electronic"))
}

func BenchmarkIDSetLoadMillion(b *testing.B) {
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		ids := new(IDSet)
		for id := int32(1); id <= 1_000_000; id++ {
			ids.Add(id)
		}
	}
}

func BenchmarkSyncMapIDSetLoadMillion(b *testing.B) {
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		ids := new(sync.Map)
		for id := int32(1); id <= 1_000_000; id++ {
			ids.Store(id, struct{}{})
		}
	}
}
