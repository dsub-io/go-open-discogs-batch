package batch

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"

	"github.com/dsub-io/go-open-discogs-batch/src/relationidentity"
	"github.com/dsub-io/open-discogs-model/model"
)

const (
	compatibilitySlotAttemptCount = uint64(1) << 32
	maxInt32Decimal               = "2147483647"
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
	return deduplicateHashedReleaseRows(
		model.ReleaseItemCreditedArtist{}.TableName(),
		relationidentity.CreditedArtist,
		items,
		func(row *model.ReleaseItemCreditedArtist) releaseTwoIntegerKey {
			return releaseTwoIntegerKey{row.ReleaseItemID, row.ArtistID, 0}
		},
		func(row *model.ReleaseItemCreditedArtist) int32 { return row.Hash },
		func(row *model.ReleaseItemCreditedArtist) relationidentity.Digest {
			return relationidentity.Sum(
				relationidentity.CreditedArtist,
				relationidentity.StringField(row.Role),
			)
		},
		func(left, right *model.ReleaseItemCreditedArtist) bool {
			return equalOptionalValue(left.Role, right.Role)
		},
		func(row *model.ReleaseItemCreditedArtist, hash int32, digest relationidentity.Digest) {
			row.Hash = hash
			row.IdentitySHA256 = modelIdentityDigest(digest)
		},
	)
}

func deduplicateReleaseWorks(
	items []*model.ReleaseItemWork,
) ([]*model.ReleaseItemWork, error) {
	return deduplicateHashedReleaseRows(
		model.ReleaseItemWork{}.TableName(),
		relationidentity.Work,
		items,
		func(row *model.ReleaseItemWork) releaseTwoIntegerKey {
			return releaseTwoIntegerKey{row.ReleaseItemID, row.LabelID, 0}
		},
		func(row *model.ReleaseItemWork) int32 { return row.Hash },
		func(row *model.ReleaseItemWork) relationidentity.Digest {
			return relationidentity.Sum(
				relationidentity.Work,
				relationidentity.StringField(row.Work),
			)
		},
		func(left, right *model.ReleaseItemWork) bool {
			return equalOptionalValue(left.Work, right.Work)
		},
		func(row *model.ReleaseItemWork, hash int32, digest relationidentity.Digest) {
			row.Hash = hash
			row.IdentitySHA256 = modelIdentityDigest(digest)
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
	for index, item := range items {
		if item == nil {
			continue
		}
		if err := canonicalizeReleaseFormatQuantity(item); err != nil {
			return nil, fmt.Errorf("invalid release_item_format quantity at batch index %d: %w", index, err)
		}
	}
	return deduplicateHashedReleaseRows(
		model.ReleaseItemFormat{}.TableName(),
		relationidentity.Format,
		items,
		func(row *model.ReleaseItemFormat) releaseIntegerKey {
			return releaseIntegerKey{row.ReleaseItemID, 0}
		},
		func(row *model.ReleaseItemFormat) int32 { return row.Hash },
		func(row *model.ReleaseItemFormat) relationidentity.Digest {
			return relationidentity.Sum(
				relationidentity.Format,
				relationidentity.StringField(row.Name),
				relationidentity.StringField(row.Description),
				releaseFormatQuantityField(row),
				relationidentity.StringField(row.Text),
			)
		},
		func(left, right *model.ReleaseItemFormat) bool {
			return equalOptionalValue(left.Description, right.Description) &&
				equalOptionalValue(left.Name, right.Name) &&
				equalOptionalValue(left.Quantity, right.Quantity) &&
				equalOptionalValue(left.QuantityText, right.QuantityText) &&
				equalOptionalValue(left.Text, right.Text)
		},
		func(row *model.ReleaseItemFormat, hash int32, digest relationidentity.Digest) {
			row.Hash = hash
			row.IdentitySHA256 = modelIdentityDigest(digest)
		},
	)
}

func canonicalizeReleaseFormatQuantity(row *model.ReleaseItemFormat) error {
	if row.QuantityText == nil {
		if row.Quantity == nil {
			return nil
		}
		if *row.Quantity < 0 {
			return fmt.Errorf("quantity must be non-negative")
		}
		canonical := strconv.FormatInt(int64(*row.Quantity), 10)
		row.QuantityText = &canonical
		return nil
	}
	canonical, integer, err := canonicalReleaseFormatQuantity(*row.QuantityText)
	if err != nil {
		return fmt.Errorf("quantity text must be a non-negative decimal")
	}
	row.QuantityText = &canonical
	if integer != nil {
		expected := *integer
		if row.Quantity != nil && *row.Quantity != expected {
			return fmt.Errorf("integer quantity does not match quantity text")
		}
		row.Quantity = &expected
		return nil
	}
	if row.Quantity != nil {
		return fmt.Errorf("oversized quantity must not have an integer value")
	}
	return nil
}

func canonicalReleaseFormatQuantity(value string) (string, *int32, error) {
	if value == "" {
		return "", nil, fmt.Errorf("quantity is empty")
	}
	for index := range len(value) {
		if value[index] < '0' || value[index] > '9' {
			return "", nil, fmt.Errorf("quantity contains a non-decimal character")
		}
	}
	firstSignificant := 0
	for firstSignificant < len(value)-1 && value[firstSignificant] == '0' {
		firstSignificant++
	}
	canonical := value[firstSignificant:]
	if len(canonical) > len(maxInt32Decimal) ||
		(len(canonical) == len(maxInt32Decimal) && canonical > maxInt32Decimal) {
		return canonical, nil, nil
	}
	var integer int32
	for index := range len(canonical) {
		integer = integer*10 + int32(canonical[index]-'0')
	}
	return canonical, &integer, nil
}

func releaseFormatQuantityField(row *model.ReleaseItemFormat) relationidentity.Field {
	return relationidentity.StringField(row.QuantityText)
}

func deduplicateReleaseIdentifiers(
	items []*model.ReleaseItemIdentifier,
) ([]*model.ReleaseItemIdentifier, error) {
	return deduplicateHashedReleaseRows(
		model.ReleaseItemIdentifier{}.TableName(),
		relationidentity.Identifier,
		items,
		func(row *model.ReleaseItemIdentifier) releaseIntegerKey {
			return releaseIntegerKey{row.ReleaseItemID, 0}
		},
		func(row *model.ReleaseItemIdentifier) int32 { return row.Hash },
		func(row *model.ReleaseItemIdentifier) relationidentity.Digest {
			return relationidentity.Sum(
				relationidentity.Identifier,
				relationidentity.StringField(row.Type),
				relationidentity.StringField(row.Description),
				relationidentity.StringField(row.Value),
			)
		},
		func(left, right *model.ReleaseItemIdentifier) bool {
			return equalOptionalValue(left.Description, right.Description) &&
				equalOptionalValue(left.Type, right.Type) &&
				equalOptionalValue(left.Value, right.Value)
		},
		func(row *model.ReleaseItemIdentifier, hash int32, digest relationidentity.Digest) {
			row.Hash = hash
			row.IdentitySHA256 = modelIdentityDigest(digest)
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
	return deduplicateHashedReleaseRows(
		model.ReleaseItemTrack{}.TableName(),
		relationidentity.Track,
		items,
		func(row *model.ReleaseItemTrack) releaseIntegerKey {
			return releaseIntegerKey{row.ReleaseItemID, 0}
		},
		func(row *model.ReleaseItemTrack) int32 { return row.Hash },
		func(row *model.ReleaseItemTrack) relationidentity.Digest {
			return relationidentity.Sum(
				relationidentity.Track,
				relationidentity.StringField(row.Position),
				relationidentity.StringField(row.Title),
				relationidentity.StringField(row.Duration),
			)
		},
		func(left, right *model.ReleaseItemTrack) bool {
			return equalOptionalValue(left.Duration, right.Duration) &&
				equalOptionalValue(left.Position, right.Position) &&
				equalOptionalValue(left.Title, right.Title)
		},
		func(row *model.ReleaseItemTrack, hash int32, digest relationidentity.Digest) {
			row.Hash = hash
			row.IdentitySHA256 = modelIdentityDigest(digest)
		},
	)
}

func deduplicateReleaseVideos(
	items []*model.ReleaseItemVideo,
) ([]*model.ReleaseItemVideo, error) {
	return deduplicateHashedReleaseRows(
		model.ReleaseItemVideo{}.TableName(),
		relationidentity.Video,
		items,
		func(row *model.ReleaseItemVideo) releaseIntegerKey {
			return releaseIntegerKey{row.ReleaseItemID, 0}
		},
		func(row *model.ReleaseItemVideo) int32 { return row.Hash },
		func(row *model.ReleaseItemVideo) relationidentity.Digest {
			return relationidentity.Sum(
				relationidentity.Video,
				relationidentity.StringField(row.Title),
				relationidentity.StringField(row.Description),
				relationidentity.StringField(row.URL),
			)
		},
		func(left, right *model.ReleaseItemVideo) bool {
			return equalOptionalValue(left.Description, right.Description) &&
				equalOptionalValue(left.Title, right.Title) &&
				equalOptionalValue(left.URL, right.URL)
		},
		func(row *model.ReleaseItemVideo, hash int32, digest relationidentity.Digest) {
			row.Hash = hash
			row.IdentitySHA256 = modelIdentityDigest(digest)
		},
	)
}

func modelIdentityDigest(digest relationidentity.Digest) *model.SHA256Digest {
	value := model.SHA256Digest(digest)
	return &value
}

type hashedReleaseRow[T any] struct {
	row    *T
	digest relationidentity.Digest
}

type hashedReleaseScope[T any] struct {
	reserved map[int32]struct{}
	groups   map[int32][]hashedReleaseRow[T]
}

func deduplicateHashedReleaseRows[T any, K comparable](
	table string,
	relation relationidentity.Relation,
	items []*T,
	scope func(*T) K,
	legacyHash func(*T) int32,
	digest func(*T) relationidentity.Digest,
	equalPayload func(*T, *T) bool,
	setIdentity func(*T, int32, relationidentity.Digest),
) ([]*T, error) {
	return deduplicateHashedReleaseRowsWithAllocator(
		table,
		relation,
		items,
		scope,
		legacyHash,
		digest,
		equalPayload,
		setIdentity,
		assignCompatibilitySlots[T],
	)
}

func deduplicateHashedReleaseRowsWithAllocator[T any, K comparable](
	table string,
	relation relationidentity.Relation,
	items []*T,
	scope func(*T) K,
	legacyHash func(*T) int32,
	digest func(*T) relationidentity.Digest,
	equalPayload func(*T, *T) bool,
	setIdentity func(*T, int32, relationidentity.Digest),
	allocate func(
		relationidentity.Relation,
		*hashedReleaseScope[T],
		func(*T, int32, relationidentity.Digest),
	) error,
) ([]*T, error) {
	rowsByIdentity := make(map[K]map[relationidentity.Digest]*T)
	scopes := make(map[K]*hashedReleaseScope[T])
	deduplicated := make([]*T, 0, len(items))
	for index, item := range items {
		if item == nil {
			return nil, fmt.Errorf("nil %s row at batch index %d", table, index)
		}
		canonicalScope := scope(item)
		canonicalDigest := digest(item)
		identityRows := rowsByIdentity[canonicalScope]
		if identityRows == nil {
			identityRows = make(map[relationidentity.Digest]*T)
			rowsByIdentity[canonicalScope] = identityRows
		}
		if previous, exists := identityRows[canonicalDigest]; exists {
			if !equalPayload(previous, item) {
				return nil, fmt.Errorf(
					"conflicting %s payloads for SHA-256 identity %x",
					table,
					canonicalDigest,
				)
			}
			if legacyHash(previous) != legacyHash(item) {
				return nil, fmt.Errorf(
					"conflicting %s legacy hashes for SHA-256 identity %x",
					table,
					canonicalDigest,
				)
			}
			continue
		}
		identityRows[canonicalDigest] = item
		canonicalHash := legacyHash(item)
		relationScope := scopes[canonicalScope]
		if relationScope == nil {
			relationScope = &hashedReleaseScope[T]{
				reserved: make(map[int32]struct{}),
				groups:   make(map[int32][]hashedReleaseRow[T]),
			}
			scopes[canonicalScope] = relationScope
		}
		relationScope.reserved[canonicalHash] = struct{}{}
		relationScope.groups[canonicalHash] = append(
			relationScope.groups[canonicalHash],
			hashedReleaseRow[T]{row: item, digest: canonicalDigest},
		)
		deduplicated = append(deduplicated, item)
	}

	for _, relationScope := range scopes {
		if err := allocate(relation, relationScope, setIdentity); err != nil {
			return nil, fmt.Errorf("allocate %s compatibility slots: %w", table, err)
		}
	}
	return deduplicated, nil
}

func assignCompatibilitySlots[T any](
	relation relationidentity.Relation,
	scope *hashedReleaseScope[T],
	setIdentity func(*T, int32, relationidentity.Digest),
) error {
	return assignCompatibilitySlotsWith(
		relation,
		scope,
		setIdentity,
		compatibilitySlotAttemptCount,
		relationidentity.CompatibilitySlot,
	)
}

func assignCompatibilitySlotsWith[T any](
	relation relationidentity.Relation,
	scope *hashedReleaseScope[T],
	setIdentity func(*T, int32, relationidentity.Digest),
	attemptCount uint64,
	generateSlot func(relationidentity.Relation, relationidentity.Digest, uint32) int32,
) error {
	legacyHashes := make([]int32, 0, len(scope.groups))
	for legacyHash := range scope.groups {
		legacyHashes = append(legacyHashes, legacyHash)
	}
	sort.Slice(legacyHashes, func(left, right int) bool {
		return legacyHashes[left] < legacyHashes[right]
	})
	assigned := make(map[int32]struct{}, len(scope.reserved))
	for _, legacyHash := range legacyHashes {
		group := scope.groups[legacyHash]
		sort.Slice(group, func(left, right int) bool {
			return bytes.Compare(group[left].digest[:], group[right].digest[:]) < 0
		})
		setIdentity(group[0].row, legacyHash, group[0].digest)
		assigned[legacyHash] = struct{}{}
		for _, collided := range group[1:] {
			allocated := false
			for attempt := uint64(0); attempt < attemptCount; attempt++ {
				candidate := generateSlot(
					relation,
					collided.digest,
					uint32(attempt),
				)
				if _, reserved := scope.reserved[candidate]; reserved {
					continue
				}
				if _, exists := assigned[candidate]; exists {
					continue
				}
				setIdentity(collided.row, candidate, collided.digest)
				assigned[candidate] = struct{}{}
				allocated = true
				break
			}
			if !allocated {
				return fmt.Errorf("signed 32-bit slot space exhausted")
			}
		}
	}
	return nil
}

func deduplicateCanonicalRows[T any, K comparable](
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
