package batch

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
)

type metadataCacheFixture struct {
	ID int32 `gorm:"column:id;primaryKey"`
}

func (metadataCacheFixture) TableName() string { return "metadata_cache_fixture" }

func TestCachedWriteMetadataFindsLockedEntry(t *testing.T) {
	modelType := reflect.TypeOf(metadataCacheFixture{})
	expected := modelWriteMetadata{columnCount: 1}
	modelWriteMetadataCache.Lock()
	modelWriteMetadataCache.values[modelType] = expected
	actual, exists := cachedWriteMetadata(modelType)
	delete(modelWriteMetadataCache.values, modelType)
	modelWriteMetadataCache.Unlock()
	require.True(t, exists)
	require.Equal(t, expected, actual)
}

func TestCanonicalBatchReturnsDedupeFailureWithoutWriting(t *testing.T) {
	expected := errors.New("fixture dedupe failure")
	actual := writeCanonicalBatch(
		[]*model.Artist{{}},
		1,
		nil,
		func([]*model.Artist) ([]*model.Artist, error) { return nil, expected },
	)
	require.ErrorIs(t, actual.Err(), expected)
}

func TestPostgresSafeBatchSize(t *testing.T) {
	tests := []struct {
		name        string
		requested   int
		columnCount int
		want        int
		wantError   string
	}{
		{name: "keeps safe request", requested: 1_000, columnCount: 15, want: 1_000},
		{name: "caps release rows", requested: 5_000, columnCount: 15, want: 4_369},
		{name: "rejects zero chunk", requested: 0, columnCount: 15, wantError: "chunk size"},
		{name: "rejects empty schema", requested: 1_000, columnCount: 0, wantError: "no columns"},
		{name: "rejects oversized schema", requested: 1, columnCount: 65_536, wantError: "too many columns"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := postgresSafeBatchSize(test.requested, test.columnCount)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestWriteMetadataCachesSchemaAndConflictClauseByModelType(t *testing.T) {
	db, _, _ := newMockGorm(t)
	first, err := writeMetadataFor(&model.Artist{}, db)
	require.NoError(t, err)
	require.Positive(t, first.columnCount)
	require.NotEmpty(t, first.conflictClause.Columns)

	second, err := writeMetadataFor(&model.Artist{}, nil)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestWriteMetadataCacheIsConcurrentAndTyped(t *testing.T) {
	db, _, _ := newMockGorm(t)
	const workers = 32
	results := make(chan modelWriteMetadata, workers)
	errors := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			metadata, err := writeMetadataFor(&metadataCacheFixture{}, db)
			results <- metadata
			errors <- err
		}()
	}
	group.Wait()
	close(results)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	for metadata := range results {
		require.Equal(t, 1, metadata.columnCount)
	}
}
