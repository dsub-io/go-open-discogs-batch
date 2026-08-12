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
		return doNothing("release_item_id", "label_id", "category_notation")
	case *model.ReleaseItemFormat:
		return updateWhenChanged(
			"release_item_format",
			[]string{"release_item_id", "hash"},
			[]string{"quantity"},
		)
	case *model.ArtistAlias:
		return doNothing("artist_id", "alias_id")
	case *model.ArtistGroup:
		return doNothing("artist_id", "group_id")
	case *model.ArtistMember:
		return doNothing("artist_id", "member_id")
	case *model.ArtistNameVariation:
		return doNothing("artist_id", "hash")
	case *model.ArtistURL:
		return doNothing("artist_id", "hash")
	case *model.LabelSubLabel:
		return doNothing("parent_label_id", "sub_label_id")
	case *model.LabelURL:
		return doNothing("label_id", "hash")
	case *model.MasterArtist:
		return doNothing("master_id", "artist_id")
	case *model.MasterGenre:
		return doNothing("master_id", "genre")
	case *model.MasterStyle:
		return doNothing("master_id", "style")
	case *model.MasterVideo:
		return doNothing("master_id", "hash")
	case *model.ReleaseItemArtist:
		return doNothing("release_item_id", "artist_id")
	case *model.ReleaseItemCreditedArtist:
		return doNothing("release_item_id", "artist_id", "hash")
	case *model.ReleaseItemGenre:
		return doNothing("release_item_id", "genre")
	case *model.ReleaseItemIdentifier:
		return doNothing("release_item_id", "hash")
	case *model.ReleaseItemImage:
		return doNothing("release_item_id", "hash")
	case *model.ReleaseItemStyle:
		return doNothing("release_item_id", "style")
	case *model.ReleaseItemTrack:
		return doNothing("release_item_id", "hash")
	case *model.ReleaseItemVideo:
		return doNothing("release_item_id", "hash")
	case *model.ReleaseItemWork:
		return doNothing("release_item_id", "label_id", "hash")
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
