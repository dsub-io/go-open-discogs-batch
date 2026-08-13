package batch

import (
	"errors"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	"github.com/dsub-io/go-open-discogs-batch/src/relationidentity"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
)

func TestReleaseTrackJavaHashCollisionPreservesBothDiscogsRows(t *testing.T) {
	const (
		releaseID  = int32(4_846_884)
		legacyHash = int32(86_171)
	)
	firstPosition := "6"
	firstTitle := "Яд"
	secondPosition := "7"
	secondTitle := "Ад"
	require.Equal(t, legacyHash, helper.JavaStringHash(firstPosition+firstTitle))
	require.Equal(t, legacyHash, helper.JavaStringHash(secondPosition+secondTitle))

	rows, err := deduplicateReleaseTracks([]*model.ReleaseItemTrack{
		{ReleaseItemID: releaseID, Hash: legacyHash, Position: &firstPosition, Title: &firstTitle},
		{ReleaseItemID: releaseID, Hash: legacyHash, Position: &secondPosition, Title: &secondTitle},
	})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, legacyHash, rows[0].Hash)
	require.Equal(t, int32(-947_370_883), rows[1].Hash)
	require.NotEqual(t, rows[0].IdentitySHA256, rows[1].IdentitySHA256)
}

func TestReleaseTrackRejectsDifferentLegacyHashesForOneIdentity(t *testing.T) {
	position := "6"
	title := "Яд"
	_, err := deduplicateReleaseTracks([]*model.ReleaseItemTrack{
		{ReleaseItemID: 4_846_884, Hash: 86_171, Position: &position, Title: &title},
		{ReleaseItemID: 4_846_884, Hash: 86_172, Position: &position, Title: &title},
	})
	require.ErrorContains(t, err, "conflicting release_item_track legacy hashes")
}

func TestReleaseFormatQuantityCanonicalizationAtTheWriteBoundary(t *testing.T) {
	require.NoError(t, canonicalizeReleaseFormatQuantity(&model.ReleaseItemFormat{}))

	quantityOne := int32(1)
	fromInteger := &model.ReleaseItemFormat{Quantity: &quantityOne}
	require.NoError(t, canonicalizeReleaseFormatQuantity(fromInteger))
	require.Equal(t, "1", *fromInteger.QuantityText)

	leadingZeroes := "0002"
	fromText := &model.ReleaseItemFormat{QuantityText: &leadingZeroes}
	require.NoError(t, canonicalizeReleaseFormatQuantity(fromText))
	require.Equal(t, "2", *fromText.QuantityText)
	require.Equal(t, int32(2), *fromText.Quantity)

	negative := "-1"
	require.Error(t, canonicalizeReleaseFormatQuantity(
		&model.ReleaseItemFormat{QuantityText: &negative},
	))
	negativeInteger := int32(-1)
	require.Error(t, canonicalizeReleaseFormatQuantity(
		&model.ReleaseItemFormat{Quantity: &negativeInteger},
	))
	mismatched := "2"
	require.Error(t, canonicalizeReleaseFormatQuantity(
		&model.ReleaseItemFormat{Quantity: &quantityOne, QuantityText: &mismatched},
	))
	oversized := "2147483648"
	require.Error(t, canonicalizeReleaseFormatQuantity(
		&model.ReleaseItemFormat{Quantity: &quantityOne, QuantityText: &oversized},
	))
	oversizedOnly := "2147483648"
	oversizedRow := &model.ReleaseItemFormat{QuantityText: &oversizedOnly}
	require.NoError(t, canonicalizeReleaseFormatQuantity(oversizedRow))
	require.Nil(t, oversizedRow.Quantity)

	rows, err := deduplicateReleaseFormats([]*model.ReleaseItemFormat{nil})
	require.Nil(t, rows)
	require.ErrorContains(t, err, "nil release_item_format row at batch index 0")
}

func TestCanonicalReleaseFormatQuantityBoundaries(t *testing.T) {
	tests := []struct {
		input     string
		canonical string
		integer   *int32
		wantError bool
	}{
		{input: "", wantError: true},
		{input: "+1", wantError: true},
		{input: "1.0", wantError: true},
		{input: "000", canonical: "0", integer: int32Pointer(0)},
		{input: "2147483647", canonical: "2147483647", integer: int32Pointer(2147483647)},
		{input: "2147483648", canonical: "2147483648"},
		{input: oversizedReleaseFormatQuantity, canonical: oversizedReleaseFormatQuantity},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			canonical, integer, err := canonicalReleaseFormatQuantity(test.input)
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.canonical, canonical)
			require.Equal(t, test.integer, integer)
		})
	}
}

func int32Pointer(value int32) *int32 {
	return &value
}

type collisionSlotFixture struct {
	legacyHash int32
	payload    string
	assigned   int32
	digest     relationidentity.Digest
}

func TestHashedReleaseDeduplicationRejectsImpossibleIdentityStates(t *testing.T) {
	first := &collisionSlotFixture{legacyHash: 1, payload: "first"}
	second := &collisionSlotFixture{legacyHash: 1, payload: "second"}
	_, err := deduplicateHashedReleaseRows(
		"fixture",
		relationidentity.Track,
		[]*collisionSlotFixture{first, second},
		func(*collisionSlotFixture) int32 { return 1 },
		func(row *collisionSlotFixture) int32 { return row.legacyHash },
		func(*collisionSlotFixture) relationidentity.Digest { return relationidentity.Digest{} },
		func(left, right *collisionSlotFixture) bool { return left.payload == right.payload },
		func(*collisionSlotFixture, int32, relationidentity.Digest) {},
	)
	require.ErrorContains(t, err, "conflicting fixture payloads for SHA-256 identity")

	expected := errors.New("fixture allocator failure")
	_, err = deduplicateHashedReleaseRowsWithAllocator(
		"fixture",
		relationidentity.Track,
		[]*collisionSlotFixture{first},
		func(*collisionSlotFixture) int32 { return 1 },
		func(row *collisionSlotFixture) int32 { return row.legacyHash },
		func(row *collisionSlotFixture) relationidentity.Digest { return row.digest },
		func(left, right *collisionSlotFixture) bool { return left.payload == right.payload },
		func(*collisionSlotFixture, int32, relationidentity.Digest) {},
		func(
			relationidentity.Relation,
			*hashedReleaseScope[collisionSlotFixture],
			func(*collisionSlotFixture, int32, relationidentity.Digest),
		) error {
			return expected
		},
	)
	require.ErrorIs(t, err, expected)
	require.ErrorContains(t, err, "allocate fixture compatibility slots")
}

func TestCompatibilitySlotAllocationSkipsUnavailableSlots(t *testing.T) {
	digest := func(value byte) relationidentity.Digest {
		var result relationidentity.Digest
		result[0] = value
		return result
	}
	rows := []*collisionSlotFixture{
		{legacyHash: 1, digest: digest(1)},
		{legacyHash: 1, digest: digest(2)},
		{legacyHash: 1, digest: digest(3)},
	}
	scope := &hashedReleaseScope[collisionSlotFixture]{
		reserved: map[int32]struct{}{1: {}, 10: {}},
		groups: map[int32][]hashedReleaseRow[collisionSlotFixture]{
			1: {
				{row: rows[0], digest: rows[0].digest},
				{row: rows[1], digest: rows[1].digest},
				{row: rows[2], digest: rows[2].digest},
			},
		},
	}
	slots := []int32{10, 20, 30}
	err := assignCompatibilitySlotsWith(
		relationidentity.Track,
		scope,
		func(row *collisionSlotFixture, slot int32, _ relationidentity.Digest) {
			row.assigned = slot
		},
		uint64(len(slots)),
		func(_ relationidentity.Relation, _ relationidentity.Digest, attempt uint32) int32 {
			return slots[attempt]
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int32{1, 20, 30}, []int32{rows[0].assigned, rows[1].assigned, rows[2].assigned})

	exhausted := &hashedReleaseScope[collisionSlotFixture]{
		reserved: map[int32]struct{}{1: {}, 10: {}},
		groups: map[int32][]hashedReleaseRow[collisionSlotFixture]{
			1: {
				{row: rows[0], digest: rows[0].digest},
				{row: rows[1], digest: rows[1].digest},
			},
		},
	}
	err = assignCompatibilitySlotsWith(
		relationidentity.Track,
		exhausted,
		func(*collisionSlotFixture, int32, relationidentity.Digest) {},
		1,
		func(relationidentity.Relation, relationidentity.Digest, uint32) int32 { return 10 },
	)
	require.ErrorContains(t, err, "signed 32-bit slot space exhausted")
}

func TestReleaseRelationBatchReturnsDedupeFailureWithoutWriting(t *testing.T) {
	expected := errors.New("fixture dedupe failure")
	actual := writeReleaseRelationBatch(
		[]*model.ReleaseItemTrack{{}},
		1,
		nil,
		func([]*model.ReleaseItemTrack) ([]*model.ReleaseItemTrack, error) {
			return nil, expected
		},
	)
	require.ErrorIs(t, actual.Err(), expected)
}

func TestReleaseRelationBatchDeduplicatesCanonicalConflictKeys(t *testing.T) {
	createdAt := time.Date(2026, time.August, 12, 1, 0, 0, 0, time.UTC)
	modifiedAt := createdAt.Add(time.Second)
	role := "Producer"
	work := "Recorded At"
	description := "description"
	name := "Vinyl"
	quantity := int32(1)
	text := "text"
	identifierType := "Barcode"
	value := "123"
	fileName := "cover.jpg"
	duration := "3:00"
	position := "A1"
	title := "Track"
	url := "https://example.invalid/video"

	t.Run("repeated artist", func(t *testing.T) {
		assertExactReleaseRelationDeduplication(t,
			deduplicateReleaseArtists,
			&model.ReleaseItemArtist{ReleaseItemID: 1, ArtistID: 2, CreatedAt: createdAt},
			&model.ReleaseItemArtist{ID: 99, ReleaseItemID: 1, ArtistID: 2, CreatedAt: modifiedAt},
		)
	})
	t.Run("credited artist hash", func(t *testing.T) {
		assertExactReleaseRelationDeduplication(t,
			deduplicateReleaseCreditedArtists,
			&model.ReleaseItemCreditedArtist{ReleaseItemID: 1, ArtistID: 2, Hash: 3, Role: &role},
			&model.ReleaseItemCreditedArtist{ID: 99, ReleaseItemID: 1, ArtistID: 2, Hash: 3, Role: &role, CreatedAt: modifiedAt},
		)
	})
	t.Run("work hash", func(t *testing.T) {
		assertExactReleaseRelationDeduplication(t,
			deduplicateReleaseWorks,
			&model.ReleaseItemWork{ReleaseItemID: 1, LabelID: 2, Hash: 3, Work: &work},
			&model.ReleaseItemWork{ID: 99, ReleaseItemID: 1, LabelID: 2, Hash: 3, Work: &work, LastModifiedAt: modifiedAt},
		)
	})
	t.Run("style", func(t *testing.T) {
		assertExactReleaseRelationDeduplication(t,
			deduplicateReleaseStyles,
			&model.ReleaseItemStyle{ReleaseItemID: 1, Style: "Techno", CreatedAt: createdAt},
			&model.ReleaseItemStyle{ID: 99, ReleaseItemID: 1, Style: "Techno", CreatedAt: modifiedAt},
		)
	})
	t.Run("genre", func(t *testing.T) {
		assertExactReleaseRelationDeduplication(t,
			deduplicateReleaseGenres,
			&model.ReleaseItemGenre{ReleaseItemID: 1, Genre: "Electronic", CreatedAt: createdAt},
			&model.ReleaseItemGenre{ID: 99, ReleaseItemID: 1, Genre: "Electronic", CreatedAt: modifiedAt},
		)
	})
	t.Run("nullable catalog number and distinct spellings", func(t *testing.T) {
		spacedCatalogNumber := "SK 026"
		compactCatalogNumber := "SK026"
		rows, err := deduplicateLabelReleaseItems([]*model.LabelReleaseItem{
			{ReleaseItemID: 1, LabelID: 2, CategoryNotation: nil, CreatedAt: createdAt},
			{ID: 99, ReleaseItemID: 1, LabelID: 2, CategoryNotation: nil, CreatedAt: modifiedAt},
			{ReleaseItemID: 1, LabelID: 2, CategoryNotation: &spacedCatalogNumber},
			{ReleaseItemID: 1, LabelID: 2, CategoryNotation: &compactCatalogNumber},
		})
		require.NoError(t, err)
		require.Len(t, rows, 3)
		require.Nil(t, rows[0].CategoryNotation)
		require.Equal(t, spacedCatalogNumber, *rows[1].CategoryNotation)
		require.Equal(t, compactCatalogNumber, *rows[2].CategoryNotation)
	})
	t.Run("format hash", func(t *testing.T) {
		assertExactReleaseRelationDeduplication(t,
			deduplicateReleaseFormats,
			&model.ReleaseItemFormat{ReleaseItemID: 1, Hash: 2, Description: &description, Name: &name, Quantity: &quantity, Text: &text},
			&model.ReleaseItemFormat{ID: 99, ReleaseItemID: 1, Hash: 2, Description: &description, Name: &name, Quantity: &quantity, Text: &text, CreatedAt: modifiedAt},
		)
	})
	t.Run("identifier hash", func(t *testing.T) {
		assertExactReleaseRelationDeduplication(t,
			deduplicateReleaseIdentifiers,
			&model.ReleaseItemIdentifier{ReleaseItemID: 1, Hash: 2, Description: &description, Type: &identifierType, Value: &value},
			&model.ReleaseItemIdentifier{ID: 99, ReleaseItemID: 1, Hash: 2, Description: &description, Type: &identifierType, Value: &value, LastModifiedAt: modifiedAt},
		)
	})
	t.Run("image hash", func(t *testing.T) {
		assertExactReleaseRelationDeduplication(t,
			deduplicateReleaseImages,
			&model.ReleaseItemImage{ReleaseItemID: 1, Hash: 2, FileName: &fileName},
			&model.ReleaseItemImage{ID: 99, ReleaseItemID: 1, Hash: 2, FileName: &fileName, CreatedAt: modifiedAt},
		)
	})
	t.Run("track hash", func(t *testing.T) {
		assertExactReleaseRelationDeduplication(t,
			deduplicateReleaseTracks,
			&model.ReleaseItemTrack{ReleaseItemID: 1, Hash: 2, Duration: &duration, Position: &position, Title: &title},
			&model.ReleaseItemTrack{ID: 99, ReleaseItemID: 1, Hash: 2, Duration: &duration, Position: &position, Title: &title, CreatedAt: modifiedAt},
		)
	})
	t.Run("video hash", func(t *testing.T) {
		assertExactReleaseRelationDeduplication(t,
			deduplicateReleaseVideos,
			&model.ReleaseItemVideo{ReleaseItemID: 1, Hash: 2, Description: &description, Title: &title, URL: &url},
			&model.ReleaseItemVideo{ID: 99, ReleaseItemID: 1, Hash: 2, Description: &description, Title: &title, URL: &url, CreatedAt: modifiedAt},
		)
	})
	t.Run("nil row", func(t *testing.T) {
		rows, err := deduplicateReleaseArtists([]*model.ReleaseItemArtist{nil})
		require.Nil(t, rows)
		require.ErrorContains(t, err, "nil release_item_artist row at batch index 0")
	})
}

func TestReleaseRelationBatchAllocatesCollisionSafeStorageSlots(t *testing.T) {
	producer := "Producer"
	remixer := "Remixer"
	recordedAt := "Recorded At"
	pressedBy := "Pressed By"
	description := "description"
	otherDescription := "other description"
	name := "Vinyl"
	quantityOne := int32(1)
	quantityTwo := int32(2)
	text := "text"
	identifierType := "Barcode"
	value := "123"
	otherValue := "456"
	fileName := "cover.jpg"
	duration := "3:00"
	position := "A1"
	title := "Track"
	otherTitle := "Other Track"
	url := "https://example.invalid/video"
	otherURL := "https://example.invalid/other-video"

	tests := []struct {
		name string
		run  func() ([]int32, [][]byte, error)
	}{
		{
			name: "credited artist",
			run: func() ([]int32, [][]byte, error) {
				rows, err := deduplicateReleaseCreditedArtists([]*model.ReleaseItemCreditedArtist{
					{ReleaseItemID: 1, ArtistID: 2, Hash: 3, Role: &producer},
					{ReleaseItemID: 1, ArtistID: 2, Hash: 3, Role: &remixer},
				})
				return hashedRowsResult(rows, err,
					func(row *model.ReleaseItemCreditedArtist) int32 { return row.Hash },
					func(row *model.ReleaseItemCreditedArtist) *model.SHA256Digest { return row.IdentitySHA256 })
			},
		},
		{
			name: "work",
			run: func() ([]int32, [][]byte, error) {
				rows, err := deduplicateReleaseWorks([]*model.ReleaseItemWork{
					{ReleaseItemID: 1, LabelID: 2, Hash: 3, Work: &recordedAt},
					{ReleaseItemID: 1, LabelID: 2, Hash: 3, Work: &pressedBy},
				})
				return hashedRowsResult(rows, err,
					func(row *model.ReleaseItemWork) int32 { return row.Hash },
					func(row *model.ReleaseItemWork) *model.SHA256Digest { return row.IdentitySHA256 })
			},
		},
		{
			name: "format",
			run: func() ([]int32, [][]byte, error) {
				rows, err := deduplicateReleaseFormats([]*model.ReleaseItemFormat{
					{ReleaseItemID: 1, Hash: 2, Description: &description, Name: &name, Quantity: &quantityOne, Text: &text},
					{ReleaseItemID: 1, Hash: 2, Description: &description, Name: &name, Quantity: &quantityTwo, Text: &text},
				})
				return hashedRowsResult(rows, err,
					func(row *model.ReleaseItemFormat) int32 { return row.Hash },
					func(row *model.ReleaseItemFormat) *model.SHA256Digest { return row.IdentitySHA256 })
			},
		},
		{
			name: "identifier",
			run: func() ([]int32, [][]byte, error) {
				rows, err := deduplicateReleaseIdentifiers([]*model.ReleaseItemIdentifier{
					{ReleaseItemID: 1, Hash: 2, Description: &description, Type: &identifierType, Value: &value},
					{ReleaseItemID: 1, Hash: 2, Description: &otherDescription, Type: &identifierType, Value: &otherValue},
				})
				return hashedRowsResult(rows, err,
					func(row *model.ReleaseItemIdentifier) int32 { return row.Hash },
					func(row *model.ReleaseItemIdentifier) *model.SHA256Digest { return row.IdentitySHA256 })
			},
		},
		{
			name: "track",
			run: func() ([]int32, [][]byte, error) {
				rows, err := deduplicateReleaseTracks([]*model.ReleaseItemTrack{
					{ReleaseItemID: 1, Hash: 2, Duration: &duration, Position: &position, Title: &title},
					{ReleaseItemID: 1, Hash: 2, Duration: &duration, Position: &position, Title: &otherTitle},
				})
				return hashedRowsResult(rows, err,
					func(row *model.ReleaseItemTrack) int32 { return row.Hash },
					func(row *model.ReleaseItemTrack) *model.SHA256Digest { return row.IdentitySHA256 })
			},
		},
		{
			name: "video",
			run: func() ([]int32, [][]byte, error) {
				rows, err := deduplicateReleaseVideos([]*model.ReleaseItemVideo{
					{ReleaseItemID: 1, Hash: 2, Description: &description, Title: &title, URL: &url},
					{ReleaseItemID: 1, Hash: 2, Description: &description, Title: &title, URL: &otherURL},
				})
				return hashedRowsResult(rows, err,
					func(row *model.ReleaseItemVideo) int32 { return row.Hash },
					func(row *model.ReleaseItemVideo) *model.SHA256Digest { return row.IdentitySHA256 })
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hashes, identities, err := test.run()
			require.NoError(t, err)
			require.Len(t, hashes, 2)
			require.NotEqual(t, hashes[0], hashes[1])
			require.Len(t, identities, 2)
			require.Len(t, identities[0], 32)
			require.Len(t, identities[1], 32)
			require.NotEqual(t, identities[0], identities[1])
		})
	}

	_, err := deduplicateReleaseImages([]*model.ReleaseItemImage{
		{ReleaseItemID: 1, Hash: 2},
		{ReleaseItemID: 1, Hash: 2, FileName: &fileName},
	})
	require.ErrorContains(t, err, "conflicting release_item_image rows")
}

func hashedRowsResult[T any](
	rows []*T,
	err error,
	hash func(*T) int32,
	identity func(*T) *model.SHA256Digest,
) ([]int32, [][]byte, error) {
	if err != nil {
		return nil, nil, err
	}
	hashes := make([]int32, 0, len(rows))
	identities := make([][]byte, 0, len(rows))
	for _, row := range rows {
		hashes = append(hashes, hash(row))
		identities = append(identities, identity(row).Bytes())
	}
	return hashes, identities, nil
}

func assertExactReleaseRelationDeduplication[T any](
	t *testing.T,
	deduplicate func([]T) ([]T, error),
	left T,
	right T,
) {
	t.Helper()
	rows, err := deduplicate([]T{left, right})
	require.NoError(t, err)
	require.Equal(t, []T{left}, rows)
}
