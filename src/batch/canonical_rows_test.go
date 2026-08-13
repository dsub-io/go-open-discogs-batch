package batch

import (
	"testing"
	"time"

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

func TestCanonicalRowsRejectConflictingPayloads(t *testing.T) {
	firstName := "first"
	secondName := "second"
	_, err := deduplicateArtists([]*model.Artist{
		{ID: 1, Name: &firstName},
		{ID: 1, Name: &secondName},
	})
	require.ErrorContains(t, err, "conflicting artist rows")

	_, err = deduplicateArtistURLs([]*model.ArtistURL{
		{ArtistID: 1, Hash: 7, URL: "first"},
		{ArtistID: 1, Hash: 7, URL: "second"},
	})
	require.ErrorContains(t, err, "conflicting artist_url rows")

	_, err = deduplicateArtistNameVariations([]*model.ArtistNameVariation{
		{ArtistID: 1, Hash: 7, NameVariation: "first"},
		{ArtistID: 1, Hash: 7, NameVariation: "second"},
	})
	require.ErrorContains(t, err, "conflicting artist_name_variation rows")

	_, err = deduplicateLabelURLs([]*model.LabelURL{
		{LabelID: 1, Hash: 7, URL: "first"},
		{LabelID: 1, Hash: 7, URL: "second"},
	})
	require.ErrorContains(t, err, "conflicting label_url rows")

	firstTitle := "first"
	secondTitle := "second"
	_, err = deduplicateMasterVideos([]*model.MasterVideo{
		{MasterID: 1, Hash: 7, Title: &firstTitle},
		{MasterID: 1, Hash: 7, Title: &secondTitle},
	})
	require.ErrorContains(t, err, "conflicting master_video rows")
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
