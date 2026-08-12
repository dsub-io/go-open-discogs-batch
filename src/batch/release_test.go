package batch

import (
	"context"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

func TestReleaseLabelsPreserveDistinctCatalogNumbers(t *testing.T) {
	cache.LabelIDs.Add(5)
	t.Cleanup(cache.ResetIDs)
	release := XmlReleaseRelation{
		ID: 2,
		Labels: []XmlLabelRelease{
			{LabelID: 5, CategoryNotation: "SK 026"},
			{LabelID: 5, CategoryNotation: "SK026"},
			{LabelID: 5, CategoryNotation: " SK026 "},
		},
	}

	labels := release.GetLabels()

	require.Len(t, labels, 2)
	require.Equal(t, "SK 026", *labels[0].CategoryNotation)
	require.Equal(t, "SK026", *labels[1].CategoryNotation)
}

func TestReleaseRelationDeduplicationFixture(t *testing.T) {
	cache.ArtistIDs.Add(1)
	cache.LabelIDs.Add(5)
	t.Cleanup(cache.ResetIDs)

	release := readReleaseRelationDeduplicationFixture(t)

	assertReleaseRelationCount(t, deduplicateReleaseArtists, release.GetReleaseArtists(), 1)
	assertReleaseRelationCount(t, deduplicateReleaseCreditedArtists, release.GetCreditedArtists(), 1)
	assertReleaseRelationCount(t, deduplicateReleaseWorks, release.GetWorks(), 1)
	assertReleaseRelationCount(t, deduplicateReleaseFormats, release.GetFormats(), 1)
	assertReleaseRelationCount(t, deduplicateReleaseGenres, release.GetReleaseGenres(), 1)
	assertReleaseRelationCount(t, deduplicateReleaseStyles, release.GetReleaseStyles(), 1)
	assertReleaseRelationCount(t, deduplicateReleaseIdentifiers, release.GetIdentifiers(), 1)
	assertReleaseRelationCount(t, deduplicateReleaseTracks, release.GetTracks(), 1)
	assertReleaseRelationCount(t, deduplicateReleaseVideos, release.GetVideos(), 1)

	labels, err := deduplicateLabelReleaseItems(release.GetLabels())
	require.NoError(t, err)
	require.Len(t, labels, 3)
	require.Nil(t, labels[0].CategoryNotation)
	require.Equal(t, "SK 026", *labels[1].CategoryNotation)
	require.Equal(t, "SK026", *labels[2].CategoryNotation)
}

func readReleaseRelationDeduplicationFixture(t *testing.T) *XmlReleaseRelation {
	t.Helper()
	file, err := os.Open("testdata/release-relations-dedup.xml")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	observable := reader.NewReader[XmlReleaseRelation](
		context.Background(),
		file,
		"release",
	)
	var release *XmlReleaseRelation
	for item := range observable.Observe() {
		require.NoError(t, item.E)
		require.NotNil(t, item.V)
		require.Nil(t, release)
		release = item.V.(*XmlReleaseRelation)
	}
	require.NotNil(t, release)
	return release
}

func assertReleaseRelationCount[T comparable](
	t *testing.T,
	deduplicate func([]T) ([]T, error),
	rows []T,
	expected int,
) {
	t.Helper()
	deduplicated, err := deduplicate(rows)
	require.NoError(t, err)
	require.Len(t, deduplicated, expected)
}

func TestReleaseRead(t *testing.T) {
	c := context.Background()
	r, err := newReadCloser("testdata/release.xml.gz", "test-read-release")
	require.NoError(t, err)
	n := "release"
	obs := reader.NewReader[XmlRelease](c, r, n)

	s := make([]*XmlRelease, 0)
	for r := range obs.Observe() {
		if r.V == nil {
			continue
		}
		x := r.V.(*XmlRelease)
		s = append(s, x)
		require.NotNil(t, x.Status)
		require.NotNil(t, x.ListedReleaseDate)
	}

	require.Len(t, s, 3)
	require.True(t, s[0].MasterInfo.IsMaster)
	require.True(t, s[1].MasterInfo.IsMaster)
	require.False(t, s[2].MasterInfo.IsMaster)
}

func TestMasterMainReleaseUpdateStatementBatchesMappings(t *testing.T) {
	updates := map[int32]int32{10: 100, 20: 200, 30: 300}

	query, arguments := masterMainReleaseUpdateStatement([]int32{10, 20, 30}, updates)

	require.Contains(t, query, "UPDATE master AS target")
	require.Equal(t, 3, strings.Count(query, "(?::integer, ?::integer)"))
	require.Len(t, arguments, 7)
	require.Equal(t, int32(10), arguments[1])
	require.Equal(t, int32(100), arguments[2])
	require.Equal(t, int32(30), arguments[5])
	require.Equal(t, int32(300), arguments[6])
}

func TestReleaseRelationRead(t *testing.T) {
	c := context.Background()
	r, err := newReadCloser("testdata/release.xml.gz", "test-read-release")
	require.NoError(t, err)
	n := "release"
	obs := reader.NewReader[XmlReleaseRelation](c, r, n)
	s := make([]*XmlReleaseRelation, 0)
	for r := range obs.Observe() {
		require.NoError(t, r.E)
		require.NotNil(t, r.V)
		s = append(s, r.V.(*XmlReleaseRelation))
	}
	require.Len(t, s, 3)
}

func TestReleaseRelationStrTim(t *testing.T) {
	emptyStr := "     "
	rel := XmlReleaseRelation{
		ID:                0,
		Title:             &emptyStr,
		Country:           &emptyStr,
		DataQuality:       &emptyStr,
		ListedReleaseDate: nil,
		Notes:             &emptyStr,
		MasterInfo:        XmlReleaseMasterInfo{},
		Status:            nil,
		Artists:           nil,
		Labels:            nil,
		CreditedArtists:   nil,
		Formats:           nil,
		Genres:            []string{"   ", ""},
		Styles:            []string{"   ", ""},
	}

	require.Len(t, rel.GetGenres(), 0, "release must return empty genres slice")
	require.Len(t, rel.GetStyles(), 0, "release must return empty styles slice")

	releaseObj := rel.GetRelease()
	require.Nil(t, releaseObj.Title)
	require.Nil(t, releaseObj.Country)
	require.Nil(t, releaseObj.DataQuality)
	require.Nil(t, releaseObj.Notes)
}
