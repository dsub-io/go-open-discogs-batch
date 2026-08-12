package batch

import (
	"fmt"

	"github.com/dsub-io/open-discogs-model/model"
)

type releaseIntegerKey struct {
	releaseItemID int32
	relationKey   int32
}

type releaseTextKey struct {
	releaseItemID int32
	relationKey   string
}

type releaseTwoIntegerKey struct {
	releaseItemID int32
	firstKey      int32
	secondKey     int32
}

type releaseLabelKey struct {
	releaseItemID    int32
	labelID          int32
	categoryNotation nullableStringKey
}

type nullableStringKey struct {
	value string
	valid bool
}

func deduplicateReleaseArtists(
	items []*model.ReleaseItemArtist,
) ([]*model.ReleaseItemArtist, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItemArtist{}.TableName(),
		items,
		func(row *model.ReleaseItemArtist) releaseIntegerKey {
			return releaseIntegerKey{row.ReleaseItemID, row.ArtistID}
		},
		func(_, _ *model.ReleaseItemArtist) bool { return true },
	)
}

func deduplicateReleaseCreditedArtists(
	items []*model.ReleaseItemCreditedArtist,
) ([]*model.ReleaseItemCreditedArtist, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItemCreditedArtist{}.TableName(),
		items,
		func(row *model.ReleaseItemCreditedArtist) releaseTwoIntegerKey {
			return releaseTwoIntegerKey{row.ReleaseItemID, row.ArtistID, row.Hash}
		},
		func(left, right *model.ReleaseItemCreditedArtist) bool {
			return equalOptionalValue(left.Role, right.Role)
		},
	)
}

func deduplicateReleaseWorks(
	items []*model.ReleaseItemWork,
) ([]*model.ReleaseItemWork, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItemWork{}.TableName(),
		items,
		func(row *model.ReleaseItemWork) releaseTwoIntegerKey {
			return releaseTwoIntegerKey{row.ReleaseItemID, row.LabelID, row.Hash}
		},
		func(left, right *model.ReleaseItemWork) bool {
			return equalOptionalValue(left.Work, right.Work)
		},
	)
}

func deduplicateReleaseStyles(
	items []*model.ReleaseItemStyle,
) ([]*model.ReleaseItemStyle, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItemStyle{}.TableName(),
		items,
		func(row *model.ReleaseItemStyle) releaseTextKey {
			return releaseTextKey{row.ReleaseItemID, row.Style}
		},
		func(_, _ *model.ReleaseItemStyle) bool { return true },
	)
}

func deduplicateReleaseGenres(
	items []*model.ReleaseItemGenre,
) ([]*model.ReleaseItemGenre, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItemGenre{}.TableName(),
		items,
		func(row *model.ReleaseItemGenre) releaseTextKey {
			return releaseTextKey{row.ReleaseItemID, row.Genre}
		},
		func(_, _ *model.ReleaseItemGenre) bool { return true },
	)
}

func deduplicateLabelReleaseItems(
	items []*model.LabelReleaseItem,
) ([]*model.LabelReleaseItem, error) {
	return deduplicateCanonicalRows(
		model.LabelReleaseItem{}.TableName(),
		items,
		func(row *model.LabelReleaseItem) releaseLabelKey {
			return releaseLabelKey{
				releaseItemID:    row.ReleaseItemID,
				labelID:          row.LabelID,
				categoryNotation: newNullableStringKey(row.CategoryNotation),
			}
		},
		func(_, _ *model.LabelReleaseItem) bool { return true },
	)
}

func deduplicateReleaseFormats(
	items []*model.ReleaseItemFormat,
) ([]*model.ReleaseItemFormat, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItemFormat{}.TableName(),
		items,
		func(row *model.ReleaseItemFormat) releaseIntegerKey {
			return releaseIntegerKey{row.ReleaseItemID, row.Hash}
		},
		func(left, right *model.ReleaseItemFormat) bool {
			return equalOptionalValue(left.Description, right.Description) &&
				equalOptionalValue(left.Name, right.Name) &&
				equalOptionalValue(left.Quantity, right.Quantity) &&
				equalOptionalValue(left.Text, right.Text)
		},
	)
}

func deduplicateReleaseIdentifiers(
	items []*model.ReleaseItemIdentifier,
) ([]*model.ReleaseItemIdentifier, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItemIdentifier{}.TableName(),
		items,
		func(row *model.ReleaseItemIdentifier) releaseIntegerKey {
			return releaseIntegerKey{row.ReleaseItemID, row.Hash}
		},
		func(left, right *model.ReleaseItemIdentifier) bool {
			return equalOptionalValue(left.Description, right.Description) &&
				equalOptionalValue(left.Type, right.Type) &&
				equalOptionalValue(left.Value, right.Value)
		},
	)
}

func deduplicateReleaseImages(
	items []*model.ReleaseItemImage,
) ([]*model.ReleaseItemImage, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItemImage{}.TableName(),
		items,
		func(row *model.ReleaseItemImage) releaseIntegerKey {
			return releaseIntegerKey{row.ReleaseItemID, row.Hash}
		},
		func(left, right *model.ReleaseItemImage) bool {
			return equalOptionalValue(left.FileName, right.FileName)
		},
	)
}

func deduplicateReleaseTracks(
	items []*model.ReleaseItemTrack,
) ([]*model.ReleaseItemTrack, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItemTrack{}.TableName(),
		items,
		func(row *model.ReleaseItemTrack) releaseIntegerKey {
			return releaseIntegerKey{row.ReleaseItemID, row.Hash}
		},
		func(left, right *model.ReleaseItemTrack) bool {
			return equalOptionalValue(left.Duration, right.Duration) &&
				equalOptionalValue(left.Position, right.Position) &&
				equalOptionalValue(left.Title, right.Title)
		},
	)
}

func deduplicateReleaseVideos(
	items []*model.ReleaseItemVideo,
) ([]*model.ReleaseItemVideo, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItemVideo{}.TableName(),
		items,
		func(row *model.ReleaseItemVideo) releaseIntegerKey {
			return releaseIntegerKey{row.ReleaseItemID, row.Hash}
		},
		func(left, right *model.ReleaseItemVideo) bool {
			return equalOptionalValue(left.Description, right.Description) &&
				equalOptionalValue(left.Title, right.Title) &&
				equalOptionalValue(left.URL, right.URL)
		},
	)
}

func deduplicateCanonicalRows[T comparable, K comparable](
	table string,
	items []*T,
	key func(*T) K,
	equalPayload func(*T, *T) bool,
) ([]*T, error) {
	seen := make(map[K]*T, len(items))
	deduplicated := make([]*T, 0, len(items))
	for index, item := range items {
		if item == nil {
			return nil, fmt.Errorf("nil %s row at batch index %d", table, index)
		}
		canonicalKey := key(item)
		previous, exists := seen[canonicalKey]
		if !exists {
			seen[canonicalKey] = item
			deduplicated = append(deduplicated, item)
			continue
		}
		if !equalPayload(previous, item) {
			return nil, fmt.Errorf(
				"conflicting %s rows for canonical key %v",
				table,
				canonicalKey,
			)
		}
	}
	return deduplicated, nil
}

func newNullableStringKey(value *string) nullableStringKey {
	if value == nil {
		return nullableStringKey{}
	}
	return nullableStringKey{value: *value, valid: true}
}

func equalOptionalValue[T comparable](left, right *T) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
