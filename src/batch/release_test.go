package batch

import (
	"context"
	"encoding/xml"
	"os"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/stretchr/testify/require"
)

const oversizedReleaseFormatQuantity = "1010487400000000000000000000000000000000000000000000"

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

func TestReleaseFormatsPreserveQuantityVariants(t *testing.T) {
	name := "CD"
	quantityOne := int32(1)
	quantityTwo := int32(2)
	release := &XmlReleaseRelation{
		ID: 48967,
		Formats: []XmlFormat{
			{Name: &name, Quantity: newXmlFormatQuantity(quantityOne), Descriptions: []string{"Compilation"}},
			{Name: &name, Quantity: newXmlFormatQuantity(quantityTwo), Descriptions: []string{"Compilation"}},
		},
	}

	formats := release.GetFormats()
	require.Len(t, formats, 2)
	require.NotEqual(t, formats[0].Hash, formats[1].Hash)
	deduplicated, err := deduplicateReleaseFormats(formats)
	require.NoError(t, err)
	require.Len(t, deduplicated, 2)
}

func TestReleaseFormatQuantityPreservesOversizedDiscogsValue(t *testing.T) {
	var release XmlReleaseRelation
	fixture := `<release id="6662697"><formats><format name="File" qty="` +
		oversizedReleaseFormatQuantity + `" text="32 kbps"/></formats></release>`
	require.NoError(t, xml.Unmarshal([]byte(fixture), &release))

	formats := release.GetFormats()
	require.Len(t, formats, 1)
	require.Nil(t, formats[0].Quantity)
	require.Equal(t, oversizedReleaseFormatQuantity, *formats[0].QuantityText)
	deduplicated, err := deduplicateReleaseFormats(formats)
	require.NoError(t, err)
	require.NotNil(t, deduplicated[0].IdentitySHA256)
	require.Len(t, deduplicated[0].IdentitySHA256.Bytes(), 32)
}

func TestReleaseFormatQuantityCanonicalizesAndRejectsInvalidValues(t *testing.T) {
	var omitted XmlReleaseRelation
	require.NoError(t, xml.Unmarshal(
		[]byte(`<release id="1"><formats><format name="CD" qty=" "/></formats></release>`),
		&omitted,
	))
	omittedFormats := omitted.GetFormats()
	require.Len(t, omittedFormats, 1)
	require.Nil(t, omittedFormats[0].Quantity)
	require.Nil(t, omittedFormats[0].QuantityText)

	var canonical XmlReleaseRelation
	require.NoError(t, xml.Unmarshal(
		[]byte(`<release id="1"><formats><format name="CD" qty="0002"/></formats></release>`),
		&canonical,
	))
	formats := canonical.GetFormats()
	require.Len(t, formats, 1)
	require.Equal(t, int32(2), *formats[0].Quantity)
	require.Equal(t, "2", *formats[0].QuantityText)

	for _, invalid := range []string{"-1", "not-a-number"} {
		var release XmlReleaseRelation
		err := xml.Unmarshal(
			[]byte(`<release id="1"><formats><format name="CD" qty="`+invalid+`"/></formats></release>`),
			&release,
		)
		require.ErrorContains(t, err, "invalid non-negative release format quantity")
	}
}

func TestReleaseRelationChunkRejectsInvalidCanonicalQuantity(t *testing.T) {
	db, _, _ := newMockGorm(t)
	order := NewOrder(context.Background(), 1, 1, "unused", db)
	name := "CD"
	invalid := XmlFormatQuantity{canonical: "-1", present: true}
	actual := writeReleaseRelationChunk(
		order,
		ChunkMetadata{},
		[]*XmlReleaseRelation{{ID: 1, Formats: []XmlFormat{{Name: &name, Quantity: invalid}}}},
	)
	require.ErrorContains(t, actual.Err(), "invalid release_item_format quantity")
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

func assertReleaseRelationCount[T any](
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
