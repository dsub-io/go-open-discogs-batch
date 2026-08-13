package batch

import (
	"fmt"
	"strings"

	"github.com/dsub-io/open-discogs-model/model"
	"gorm.io/gorm/clause"
)

func ExtractClause(item any) clause.OnConflict {
	switch item.(type) {
	case *model.Artist:
		return updateWhenChanged(
			"artist",
			[]string{"id"},
			[]string{"data_quality", "name", "profile", "real_name"},
		)
	case *model.Label:
		return updateWhenChanged(
			"label",
			[]string{"id"},
			[]string{"contact_info", "data_quality", "name", "profile"},
		)
	case *model.Master:
		return updateWhenChanged(
			"master",
			[]string{"id"},
			[]string{"data_quality", "title", "year"},
		)
	case *model.ReleaseItem:
		return updateWhenChanged(
			"release_item",
			[]string{"id"},
			[]string{
				"country",
				"data_quality",
				"has_valid_day",
				"has_valid_month",
				"has_valid_year",
				"is_master",
				"master_id",
				"listed_release_date",
				"notes",
				"release_date",
				"status",
				"title",
			},
		)
	case *model.LabelReleaseItem:
		return updateOrdinalWhenChanged("label_release_item", "release_item_id", "label_id", "category_notation")
	case *model.ReleaseItemFormat:
		return updateOrdinalWhenChanged("release_item_format", "release_item_id", "hash")
	case *model.ArtistAlias:
		return updateOrdinalWhenChanged("artist_alias", "artist_id", "alias_id")
	case *model.ArtistGroup:
		return updateOrdinalWhenChanged("artist_group", "artist_id", "group_id")
	case *model.ArtistMember:
		return updateOrdinalWhenChanged("artist_member", "artist_id", "member_id")
	case *model.ArtistNameVariation:
		return updateOrdinalWhenChanged("artist_name_variation", "artist_id", "hash")
	case *model.ArtistURL:
		return updateOrdinalWhenChanged("artist_url", "artist_id", "hash")
	case *model.LabelSubLabel:
		return updateOrdinalWhenChanged("label_sub_label", "parent_label_id", "sub_label_id")
	case *model.LabelURL:
		return updateOrdinalWhenChanged("label_url", "label_id", "hash")
	case *model.MasterArtist:
		return updateOrdinalWhenChanged("master_artist", "master_id", "artist_id")
	case *model.MasterGenre:
		return updateOrdinalWhenChanged("master_genre", "master_id", "genre")
	case *model.MasterStyle:
		return updateOrdinalWhenChanged("master_style", "master_id", "style")
	case *model.MasterVideo:
		return updateOrdinalWhenChanged("master_video", "master_id", "hash")
	case *model.ReleaseItemArtist:
		return updateOrdinalWhenChanged("release_item_artist", "release_item_id", "artist_id")
	case *model.ReleaseItemCreditedArtist:
		return updateOrdinalWhenChanged("release_item_credited_artist", "release_item_id", "artist_id", "hash")
	case *model.ReleaseItemGenre:
		return updateOrdinalWhenChanged("release_item_genre", "release_item_id", "genre")
	case *model.ReleaseItemIdentifier:
		return updateOrdinalWhenChanged("release_item_identifier", "release_item_id", "hash")
	case *model.ReleaseItemImage:
		return updateOrdinalWhenChanged("release_item_image", "release_item_id", "hash")
	case *model.ReleaseItemStyle:
		return updateOrdinalWhenChanged("release_item_style", "release_item_id", "style")
	case *model.ReleaseItemTrack:
		return updateOrdinalWhenChanged("release_item_track", "release_item_id", "hash")
	case *model.ReleaseItemVideo:
		return updateOrdinalWhenChanged("release_item_video", "release_item_id", "hash")
	case *model.ReleaseItemWork:
		return updateOrdinalWhenChanged("release_item_work", "release_item_id", "label_id", "hash")
	case *model.Genre:
		return doNothing("name")
	case *model.Style:
		return doNothing("name")
	default:
		return clause.OnConflict{DoNothing: true}
	}
}

func doNothing(columns ...string) clause.OnConflict {
	return clause.OnConflict{
		Columns:   clauseColumns(columns),
		DoNothing: true,
	}
}

func updateOrdinalWhenChanged(table string, conflictColumns ...string) clause.OnConflict {
	return updateWhenChanged(table, conflictColumns, []string{"ordinal"})
}

func updateWhenChanged(
	table string,
	conflictColumns []string,
	businessColumns []string,
) clause.OnConflict {
	updateColumns := append(append([]string{}, businessColumns...), "last_modified_at")
	distinct := make([]string, len(businessColumns))
	for index, column := range businessColumns {
		distinct[index] = fmt.Sprintf(
			`"%s"."%s" IS DISTINCT FROM excluded."%s"`,
			table,
			column,
			column,
		)
	}
	return clause.OnConflict{
		Columns:   clauseColumns(conflictColumns),
		DoUpdates: clause.AssignmentColumns(updateColumns),
		Where: clause.Where{Exprs: []clause.Expression{
			clause.Expr{SQL: strings.Join(distinct, " OR ")},
		}},
	}
}

func clauseColumns(columns []string) []clause.Column {
	result := make([]clause.Column, len(columns))
	for index, column := range columns {
		result[index] = clause.Column{Name: column}
	}
	return result
}
