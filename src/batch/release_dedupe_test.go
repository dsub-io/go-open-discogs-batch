package batch

import (
	"context"
	"testing"
	"time"

	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
)

func TestWriteReleaseRelationChunkRejectsHashCollisionBeforeDatabaseStatements(t *testing.T) {
	db, mock, _ := newMockGorm(t)
	mock.ExpectBegin()
	mock.ExpectRollback()
	left := "Aa"
	right := "BB"
	actual := writeReleaseRelationChunk(
		NewOrder(context.Background(), 10, 1, "unused", db),
		ChunkMetadata{},
		[]*XmlReleaseRelation{{
			ID: 1,
			Formats: []XmlFormat{
				{Name: &left},
				{Name: &right},
			},
		}},
		false,
	)

	require.ErrorContains(t, actual.Err(), "conflicting release_item_format rows")
	require.ErrorContains(t, actual.Err(), "canonical key")
	require.NoError(t, mock.ExpectationsWereMet())
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

func TestReleaseRelationBatchRejectsCanonicalHashCollisions(t *testing.T) {
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
		run  func() error
	}{
		{
			name: "credited artist",
			run: func() error {
				_, err := deduplicateReleaseCreditedArtists([]*model.ReleaseItemCreditedArtist{
					{ReleaseItemID: 1, ArtistID: 2, Hash: 3, Role: &producer},
					{ReleaseItemID: 1, ArtistID: 2, Hash: 3, Role: &remixer},
				})
				return err
			},
		},
		{
			name: "work",
			run: func() error {
				_, err := deduplicateReleaseWorks([]*model.ReleaseItemWork{
					{ReleaseItemID: 1, LabelID: 2, Hash: 3, Work: &recordedAt},
					{ReleaseItemID: 1, LabelID: 2, Hash: 3, Work: &pressedBy},
				})
				return err
			},
		},
		{
			name: "format",
			run: func() error {
				_, err := deduplicateReleaseFormats([]*model.ReleaseItemFormat{
					{ReleaseItemID: 1, Hash: 2, Description: &description, Name: &name, Quantity: &quantityOne, Text: &text},
					{ReleaseItemID: 1, Hash: 2, Description: &description, Name: &name, Quantity: &quantityTwo, Text: &text},
				})
				return err
			},
		},
		{
			name: "identifier",
			run: func() error {
				_, err := deduplicateReleaseIdentifiers([]*model.ReleaseItemIdentifier{
					{ReleaseItemID: 1, Hash: 2, Description: &description, Type: &identifierType, Value: &value},
					{ReleaseItemID: 1, Hash: 2, Description: &otherDescription, Type: &identifierType, Value: &otherValue},
				})
				return err
			},
		},
		{
			name: "image",
			run: func() error {
				_, err := deduplicateReleaseImages([]*model.ReleaseItemImage{
					{ReleaseItemID: 1, Hash: 2},
					{ReleaseItemID: 1, Hash: 2, FileName: &fileName},
				})
				return err
			},
		},
		{
			name: "track",
			run: func() error {
				_, err := deduplicateReleaseTracks([]*model.ReleaseItemTrack{
					{ReleaseItemID: 1, Hash: 2, Duration: &duration, Position: &position, Title: &title},
					{ReleaseItemID: 1, Hash: 2, Duration: &duration, Position: &position, Title: &otherTitle},
				})
				return err
			},
		},
		{
			name: "video",
			run: func() error {
				_, err := deduplicateReleaseVideos([]*model.ReleaseItemVideo{
					{ReleaseItemID: 1, Hash: 2, Description: &description, Title: &title, URL: &url},
					{ReleaseItemID: 1, Hash: 2, Description: &description, Title: &title, URL: &otherURL},
				})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.run()
			require.Error(t, err)
			require.ErrorContains(t, err, "conflicting release_item_")
			require.ErrorContains(t, err, "canonical key")
		})
	}
}

func assertExactReleaseRelationDeduplication[T comparable](
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
