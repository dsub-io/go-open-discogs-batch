package batch

import (
	"github.com/dsub-io/open-discogs-model/model"
)

type twoIntegerKey struct {
	first  int32
	second int32
}

type integerTextKey struct {
	integer int32
	text    string
}

func deduplicateComparable[T comparable](items []T) []T {
	seen := make(map[T]struct{}, len(items))
	deduplicated := make([]T, 0, len(items))
	for _, item := range items {
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		deduplicated = append(deduplicated, item)
	}
	return deduplicated
}

func deduplicateArtists(items []*model.Artist) ([]*model.Artist, error) {
	return deduplicateCanonicalRows(
		model.Artist{}.TableName(),
		items,
		func(row *model.Artist) int32 { return row.ID },
		func(left, right *model.Artist) bool {
			return equalOptionalValue(left.DataQuality, right.DataQuality) &&
				equalOptionalValue(left.Name, right.Name) &&
				equalOptionalValue(left.Profile, right.Profile) &&
				equalOptionalValue(left.RealName, right.RealName)
		},
	)
}

func deduplicateArtistURLs(items []*model.ArtistURL) ([]*model.ArtistURL, error) {
	return deduplicateCanonicalRows(
		model.ArtistURL{}.TableName(),
		items,
		func(row *model.ArtistURL) twoIntegerKey {
			return twoIntegerKey{row.ArtistID, row.Hash}
		},
		func(left, right *model.ArtistURL) bool { return left.URL == right.URL },
	)
}

func deduplicateArtistAliases(items []*model.ArtistAlias) ([]*model.ArtistAlias, error) {
	return deduplicateCanonicalRows(
		model.ArtistAlias{}.TableName(),
		items,
		func(row *model.ArtistAlias) twoIntegerKey {
			return twoIntegerKey{row.ArtistID, row.AliasID}
		},
		func(_, _ *model.ArtistAlias) bool { return true },
	)
}

func deduplicateArtistGroups(items []*model.ArtistGroup) ([]*model.ArtistGroup, error) {
	return deduplicateCanonicalRows(
		model.ArtistGroup{}.TableName(),
		items,
		func(row *model.ArtistGroup) twoIntegerKey {
			return twoIntegerKey{row.ArtistID, row.GroupID}
		},
		func(_, _ *model.ArtistGroup) bool { return true },
	)
}

func deduplicateArtistMembers(items []*model.ArtistMember) ([]*model.ArtistMember, error) {
	return deduplicateCanonicalRows(
		model.ArtistMember{}.TableName(),
		items,
		func(row *model.ArtistMember) twoIntegerKey {
			return twoIntegerKey{row.ArtistID, row.MemberID}
		},
		func(_, _ *model.ArtistMember) bool { return true },
	)
}

func deduplicateArtistNameVariations(
	items []*model.ArtistNameVariation,
) ([]*model.ArtistNameVariation, error) {
	return deduplicateCanonicalRows(
		model.ArtistNameVariation{}.TableName(),
		items,
		func(row *model.ArtistNameVariation) twoIntegerKey {
			return twoIntegerKey{row.ArtistID, row.Hash}
		},
		func(left, right *model.ArtistNameVariation) bool {
			return left.NameVariation == right.NameVariation
		},
	)
}

func deduplicateLabels(items []*model.Label) ([]*model.Label, error) {
	return deduplicateCanonicalRows(
		model.Label{}.TableName(),
		items,
		func(row *model.Label) int32 { return row.ID },
		func(left, right *model.Label) bool {
			return equalOptionalValue(left.ContactInfo, right.ContactInfo) &&
				equalOptionalValue(left.DataQuality, right.DataQuality) &&
				equalOptionalValue(left.Name, right.Name) &&
				equalOptionalValue(left.Profile, right.Profile)
		},
	)
}

func deduplicateLabelURLs(items []*model.LabelURL) ([]*model.LabelURL, error) {
	return deduplicateCanonicalRows(
		model.LabelURL{}.TableName(),
		items,
		func(row *model.LabelURL) twoIntegerKey {
			return twoIntegerKey{row.LabelID, row.Hash}
		},
		func(left, right *model.LabelURL) bool { return left.URL == right.URL },
	)
}

func deduplicateLabelSubLabels(items []*model.LabelSubLabel) ([]*model.LabelSubLabel, error) {
	return deduplicateCanonicalRows(
		model.LabelSubLabel{}.TableName(),
		items,
		func(row *model.LabelSubLabel) twoIntegerKey {
			return twoIntegerKey{row.ParentLabelID, row.SubLabelID}
		},
		func(_, _ *model.LabelSubLabel) bool { return true },
	)
}

func deduplicateMasters(items []*model.Master) ([]*model.Master, error) {
	return deduplicateCanonicalRows(
		model.Master{}.TableName(),
		items,
		func(row *model.Master) int32 { return row.ID },
		func(left, right *model.Master) bool {
			return equalOptionalValue(left.DataQuality, right.DataQuality) &&
				equalOptionalValue(left.Title, right.Title) &&
				equalOptionalValue(left.Year, right.Year)
		},
	)
}

func deduplicateMasterArtists(items []*model.MasterArtist) ([]*model.MasterArtist, error) {
	return deduplicateCanonicalRows(
		model.MasterArtist{}.TableName(),
		items,
		func(row *model.MasterArtist) twoIntegerKey {
			return twoIntegerKey{row.MasterID, row.ArtistID}
		},
		func(_, _ *model.MasterArtist) bool { return true },
	)
}

func deduplicateMasterGenres(items []*model.MasterGenre) ([]*model.MasterGenre, error) {
	return deduplicateCanonicalRows(
		model.MasterGenre{}.TableName(),
		items,
		func(row *model.MasterGenre) integerTextKey {
			return integerTextKey{row.MasterID, row.Genre}
		},
		func(_, _ *model.MasterGenre) bool { return true },
	)
}

func deduplicateMasterStyles(items []*model.MasterStyle) ([]*model.MasterStyle, error) {
	return deduplicateCanonicalRows(
		model.MasterStyle{}.TableName(),
		items,
		func(row *model.MasterStyle) integerTextKey {
			return integerTextKey{row.MasterID, row.Style}
		},
		func(_, _ *model.MasterStyle) bool { return true },
	)
}

func deduplicateMasterVideos(items []*model.MasterVideo) ([]*model.MasterVideo, error) {
	return deduplicateCanonicalRows(
		model.MasterVideo{}.TableName(),
		items,
		func(row *model.MasterVideo) twoIntegerKey {
			return twoIntegerKey{row.MasterID, row.Hash}
		},
		func(left, right *model.MasterVideo) bool {
			return equalOptionalValue(left.Description, right.Description) &&
				equalOptionalValue(left.Title, right.Title) &&
				equalOptionalValue(left.URL, right.URL)
		},
	)
}

func deduplicateReleaseItems(items []*model.ReleaseItem) ([]*model.ReleaseItem, error) {
	return deduplicateCanonicalRows(
		model.ReleaseItem{}.TableName(),
		items,
		func(row *model.ReleaseItem) int32 { return row.ID },
		func(left, right *model.ReleaseItem) bool {
			return equalOptionalValue(left.Country, right.Country) &&
				equalOptionalValue(left.DataQuality, right.DataQuality) &&
				equalOptionalValue(left.HasValidDay, right.HasValidDay) &&
				equalOptionalValue(left.HasValidMonth, right.HasValidMonth) &&
				equalOptionalValue(left.HasValidYear, right.HasValidYear) &&
				equalOptionalValue(left.IsMaster, right.IsMaster) &&
				equalOptionalValue(left.MasterID, right.MasterID) &&
				equalOptionalValue(left.ListedReleaseDate, right.ListedReleaseDate) &&
				equalOptionalValue(left.Notes, right.Notes) &&
				equalOptionalValue(left.ReleaseDate, right.ReleaseDate) &&
				equalOptionalValue(left.Status, right.Status) &&
				equalOptionalValue(left.Title, right.Title)
		},
	)
}

func deduplicateGenres(items []*model.Genre) ([]*model.Genre, error) {
	return deduplicateCanonicalRows(
		model.Genre{}.TableName(),
		items,
		func(row *model.Genre) string { return row.Name },
		func(_, _ *model.Genre) bool { return true },
	)
}

func deduplicateStyles(items []*model.Style) ([]*model.Style, error) {
	return deduplicateCanonicalRows(
		model.Style{}.TableName(),
		items,
		func(row *model.Style) string { return row.Name },
		func(_, _ *model.Style) bool { return true },
	)
}
