package batch

import (
	"context"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
)

func TestSimpleSourceChunkUsesOneObservedAt(t *testing.T) {
	cache.ResetIDs()
	t.Cleanup(cache.ResetIDs)
	transform := transformSourceChunk((*XmlArtist).TransformAt)
	value, err := transform(context.Background(), []*XmlArtist{
		{ID: 1},
		nil,
		{ID: 2},
	})
	require.NoError(t, err)
	rows := value.([]*model.Artist)
	require.Len(t, rows, 2)
	require.False(t, rows[0].CreatedAt.IsZero())
	require.Equal(t, rows[0].CreatedAt, rows[0].LastModifiedAt)
	require.Equal(t, rows[0].CreatedAt, rows[1].CreatedAt)
	require.True(t, cache.ArtistIDs.Contains(1))
	require.True(t, cache.ArtistIDs.Contains(2))
}

func TestSimpleSourceChunkHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	transform := transformSourceChunk((*XmlArtist).TransformAt)
	_, err := transform(ctx, []*XmlArtist{{ID: 1}})
	require.ErrorIs(t, err, context.Canceled)
}

func TestRelationRowsUseSourceObservedAt(t *testing.T) {
	observedAt := time.Unix(123, 0).UTC()
	artist := &XmlArtistRelation{
		ID:         1,
		URLs:       []string{"first", "second"},
		observedAt: observedAt,
	}
	rows := artist.GetUrls()
	require.Len(t, rows, 2)
	for _, row := range rows {
		require.Equal(t, observedAt, row.CreatedAt)
		require.Equal(t, observedAt, row.LastModifiedAt)
	}
	require.Equal(t, observedAt, artist.GetArtist().CreatedAt)
}

func TestRelationChunkAssignsOneObservedAtBeforeCanonicalization(t *testing.T) {
	first := &XmlArtistRelation{ID: 1, URLs: []string{"Aa", "BB"}}
	second := &XmlArtistRelation{ID: 2}
	db, mock, _ := newMockGorm(t)
	actual := writeArtistRelationChunk(
		NewOrder(context.Background(), 100, 1, "unused", db),
		ChunkMetadata{},
		[]*XmlArtistRelation{first, second},
	)
	require.ErrorContains(t, actual.Err(), "conflicting artist_url rows")
	require.False(t, first.observedAt.IsZero())
	require.Equal(t, first.observedAt, second.observedAt)
	require.NoError(t, mock.ExpectationsWereMet())
}
