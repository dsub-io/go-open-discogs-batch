package batch

import (
	"errors"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
)

func requireOneCanonicalRow[T any](
	t *testing.T,
	deduplicate func([]*T) ([]*T, error),
	rows ...*T,
) {
	t.Helper()
	actual, err := deduplicate(rows)
	require.NoError(t, err)
	require.Len(t, actual, 1)
	require.Same(t, rows[0], actual[0])
}

func TestCanonicalRowsIgnoreDatabaseManagedColumns(t *testing.T) {
	firstTimestamp := time.Unix(1, 0).UTC()
	secondTimestamp := time.Unix(2, 0).UTC()
	name := "Artist"
	first := &model.Artist{
		ID: 1, CreatedAt: firstTimestamp, LastModifiedAt: firstTimestamp, Name: &name,
	}
	second := &model.Artist{
		ID: 1, CreatedAt: secondTimestamp, LastModifiedAt: secondTimestamp, Name: &name,
	}
	requireOneCanonicalRow(t, deduplicateArtists, first, second)

	firstURL := &model.ArtistURL{
		ArtistID: 1, Hash: 2, URL: "https://example.test", LastModifiedAt: firstTimestamp,
	}
	secondURL := &model.ArtistURL{
		ArtistID: 1, Hash: 2, URL: "https://example.test", LastModifiedAt: secondTimestamp,
	}
	requireOneCanonicalRow(t, deduplicateArtistURLs, firstURL, secondURL)
}

func TestCanonicalRootRowsRejectConflictingPayloads(t *testing.T) {
	firstName := "first"
	secondName := "second"
	_, err := deduplicateArtists([]*model.Artist{
		{ID: 1, Name: &firstName},
		{ID: 1, Name: &secondName},
	})
	require.ErrorContains(t, err, "conflicting artist rows")

}

func TestCatalogHashCollisionsPreserveDistinctPayloads(t *testing.T) {
	firstTitle := "first"
	secondTitle := "second"
	tests := []struct {
		name   string
		verify func(*testing.T)
	}{
		{
			name: "artist URL",
			verify: func(t *testing.T) {
				rows, err := deduplicateArtistURLs([]*model.ArtistURL{
					{ArtistID: 1, Hash: 7, URL: "first"},
					{ArtistID: 1, Hash: 7, URL: "second"},
				})
				require.NoError(t, err)
				require.Len(t, rows, 2)
				requireCatalogCollisionPreserved(t, rows[0].Hash, rows[1].Hash,
					rows[0].IdentitySHA256, rows[1].IdentitySHA256)
			},
		},
		{
			name: "artist name variation",
			verify: func(t *testing.T) {
				rows, err := deduplicateArtistNameVariations([]*model.ArtistNameVariation{
					{ArtistID: 1, Hash: 7, NameVariation: "first"},
					{ArtistID: 1, Hash: 7, NameVariation: "second"},
				})
				require.NoError(t, err)
				require.Len(t, rows, 2)
				requireCatalogCollisionPreserved(t, rows[0].Hash, rows[1].Hash,
					rows[0].IdentitySHA256, rows[1].IdentitySHA256)
			},
		},
		{
			name: "label URL",
			verify: func(t *testing.T) {
				rows, err := deduplicateLabelURLs([]*model.LabelURL{
					{LabelID: 1, Hash: 7, URL: "first"},
					{LabelID: 1, Hash: 7, URL: "second"},
				})
				require.NoError(t, err)
				require.Len(t, rows, 2)
				requireCatalogCollisionPreserved(t, rows[0].Hash, rows[1].Hash,
					rows[0].IdentitySHA256, rows[1].IdentitySHA256)
			},
		},
		{
			name: "master video",
			verify: func(t *testing.T) {
				rows, err := deduplicateMasterVideos([]*model.MasterVideo{
					{MasterID: 1, Hash: 7, Title: &firstTitle},
					{MasterID: 1, Hash: 7, Title: &secondTitle},
				})
				require.NoError(t, err)
				require.Len(t, rows, 2)
				requireCatalogCollisionPreserved(t, rows[0].Hash, rows[1].Hash,
					rows[0].IdentitySHA256, rows[1].IdentitySHA256)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, test.verify)
	}
}

func TestArtistNameVariationPreservesProductionHashCollision(t *testing.T) {
	const collisionHash = int32(-1130078775)

	rows := (&XmlArtistRelation{
		ID:       33476,
		NameVars: []string{"Al Thompson", "C. Thompson"},
	}).GetNameVars()
	require.Len(t, rows, 2)
	require.Equal(t, collisionHash, rows[0].Hash)
	require.Equal(t, collisionHash, rows[1].Hash)

	deduplicated, err := deduplicateArtistNameVariations(rows)
	require.NoError(t, err)
	require.Len(t, deduplicated, 2)
	requireCatalogCollisionPreserved(
		t,
		deduplicated[0].Hash,
		deduplicated[1].Hash,
		deduplicated[0].IdentitySHA256,
		deduplicated[1].IdentitySHA256,
	)
}

func requireCatalogCollisionPreserved(
	t *testing.T,
	firstHash, secondHash int32,
	firstIdentity, secondIdentity *model.SHA256Digest,
) {
	t.Helper()
	require.NotEqual(t, firstHash, secondHash)
	require.NotNil(t, firstIdentity)
	require.NotNil(t, secondIdentity)
	require.NotEqual(t, firstIdentity, secondIdentity)
}

func TestRelationChunkTransformsRejectConflictingCanonicalRowsBeforeDatabaseAccess(t *testing.T) {
	first := "first"
	second := "second"
	tests := []struct {
		name  string
		write func() error
	}{
		{
			name: "master",
			write: func() error {
				return writeMasterRelationChunk(nil, ChunkMetadata{}, []*XmlMasterRelation{
					{ID: 1, Title: &first},
					{ID: 1, Title: &second},
				}).Err()
			},
		},
		{
			name: "release",
			write: func() error {
				return writeReleaseRelationChunk(nil, ChunkMetadata{}, []*XmlReleaseRelation{
					{ID: 1, Title: &first},
					{ID: 1, Title: &second},
				}).Err()
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorContains(t, test.write(), "conflicting")
		})
	}
}

func TestEveryNonReleaseCanonicalKeyCollapsesExactDuplicates(t *testing.T) {
	requireOneCanonicalRow(t, deduplicateArtistAliases,
		&model.ArtistAlias{ArtistID: 1, AliasID: 2},
		&model.ArtistAlias{ArtistID: 1, AliasID: 2},
	)
	requireOneCanonicalRow(t, deduplicateArtistGroups,
		&model.ArtistGroup{ArtistID: 1, GroupID: 2},
		&model.ArtistGroup{ArtistID: 1, GroupID: 2},
	)
	requireOneCanonicalRow(t, deduplicateArtistMembers,
		&model.ArtistMember{ArtistID: 1, MemberID: 2},
		&model.ArtistMember{ArtistID: 1, MemberID: 2},
	)
	requireOneCanonicalRow(t, deduplicateArtistNameVariations,
		&model.ArtistNameVariation{ArtistID: 1, Hash: 2, NameVariation: "name"},
		&model.ArtistNameVariation{ArtistID: 1, Hash: 2, NameVariation: "name"},
	)
	requireOneCanonicalRow(t, deduplicateLabels,
		&model.Label{ID: 1},
		&model.Label{ID: 1},
	)
	requireOneCanonicalRow(t, deduplicateLabelURLs,
		&model.LabelURL{LabelID: 1, Hash: 2, URL: "url"},
		&model.LabelURL{LabelID: 1, Hash: 2, URL: "url"},
	)
	requireOneCanonicalRow(t, deduplicateLabelSubLabels,
		&model.LabelSubLabel{ParentLabelID: 1, SubLabelID: 2},
		&model.LabelSubLabel{ParentLabelID: 1, SubLabelID: 2},
	)
	requireOneCanonicalRow(t, deduplicateMasters,
		&model.Master{ID: 1},
		&model.Master{ID: 1},
	)
	requireOneCanonicalRow(t, deduplicateMasterArtists,
		&model.MasterArtist{MasterID: 1, ArtistID: 2},
		&model.MasterArtist{MasterID: 1, ArtistID: 2},
	)
	requireOneCanonicalRow(t, deduplicateMasterGenres,
		&model.MasterGenre{MasterID: 1, Genre: "genre"},
		&model.MasterGenre{MasterID: 1, Genre: "genre"},
	)
	requireOneCanonicalRow(t, deduplicateMasterStyles,
		&model.MasterStyle{MasterID: 1, Style: "style"},
		&model.MasterStyle{MasterID: 1, Style: "style"},
	)
	requireOneCanonicalRow(t, deduplicateMasterVideos,
		&model.MasterVideo{MasterID: 1, Hash: 2},
		&model.MasterVideo{MasterID: 1, Hash: 2},
	)
	requireOneCanonicalRow(t, deduplicateReleaseItems,
		&model.ReleaseItem{ID: 1},
		&model.ReleaseItem{ID: 1},
	)
	requireOneCanonicalRow(t, deduplicateGenres,
		&model.Genre{Name: "genre"},
		&model.Genre{Name: "genre"},
	)
	requireOneCanonicalRow(t, deduplicateStyles,
		&model.Style{Name: "style"},
		&model.Style{Name: "style"},
	)
}

func TestDeduplicateComparablePreservesFirstSeenOrder(t *testing.T) {
	require.Equal(t, []int32{3, 1, 2}, deduplicateComparable([]int32{3, 1, 3, 2, 1}))
}

func TestAfterCanonicalizationStopsBeforePersistence(t *testing.T) {
	expected := errors.New("fixture")
	actual := afterCanonicalization(expected, func() result.Result {
		t.Fatal("persistence must not run after canonicalization fails")
		return nil
	})
	require.ErrorIs(t, actual.Err(), expected)
}
