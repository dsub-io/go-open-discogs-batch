package batch

import (
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/go-open-discogs-batch/src/unique"
	"github.com/dsub-io/open-discogs-model/model"
)

var (
	artistAliasRelation = integerRelation{
		table: "artist_alias", parentColumn: "artist_id", keyColumn: "alias_id",
	}
	artistGroupRelation = integerRelation{
		table: "artist_group", parentColumn: "artist_id", keyColumn: "group_id",
	}
	artistMemberRelation = integerRelation{
		table: "artist_member", parentColumn: "artist_id", keyColumn: "member_id",
	}
	artistNameVariationRelation = integerRelation{
		table: "artist_name_variation", parentColumn: "artist_id", keyColumn: "hash",
	}
	artistURLRelation = integerRelation{
		table: "artist_url", parentColumn: "artist_id", keyColumn: "hash",
	}
)

func GetArtistStep(order Order) Step {
	return func() result.Result {
		updated := 0
		res := insertArtists(order)
		updated += res.Count()
		if res.IsErr() {
			return result.NewResult(updated, res.Err())
		}
		res = insertArtistRelations(order)
		updated += res.Count()
		if res.IsErr() {
			return result.NewResult(updated, res.Err())
		}
		return result.NewResult(updated, nil)
	}
}

func insertArtists(order Order) result.Result {
	return InsertSimple[XmlArtist, model.Artist](order, "artists", "artist")
}

func insertArtistRelations(order Order) result.Result {
	deleteStale, err := relationTablesContainRows(
		order,
		artistAliasRelation.table,
		artistGroupRelation.table,
		artistMemberRelation.table,
		artistNameVariationRelation.table,
		artistURLRelation.table,
	)
	if err != nil {
		return result.NewResult(0, err)
	}
	return processRelationChunks(
		order,
		"artist relations",
		"artist",
		"source-read artist relations",
		func(order Order, chunk ChunkMetadata, items []*XmlArtistRelation) result.Result {
			return writeArtistRelationChunk(order, chunk, items, deleteStale)
		},
	)
}

func writeArtistRelationChunk(
	order Order,
	chunk ChunkMetadata,
	items []*XmlArtistRelation,
	deleteStale bool,
) result.Result {
	return executeChunk(order, chunk, func(transactionOrder Order) result.Result {
		rootIDs := make([]int32, 0, len(items))
		artists := make([]*model.Artist, 0, len(items))
		nameVariations := make([]*model.ArtistNameVariation, 0)
		aliases := make([]*model.ArtistAlias, 0)
		groups := make([]*model.ArtistGroup, 0)
		members := make([]*model.ArtistMember, 0)
		urls := make([]*model.ArtistURL, 0)
		for _, item := range items {
			if item == nil {
				continue
			}
			rootIDs = append(rootIDs, item.ID)
			artists = append(artists, item.GetArtist())
			aliases = append(aliases, item.GetAliases()...)
			groups = append(groups, item.GetGroups()...)
			members = append(members, item.GetMembers()...)
			nameVariations = append(nameVariations, item.GetNameVars()...)
			urls = append(urls, item.GetUrls()...)
		}
		rootIDs = unique.Slice(rootIDs)
		written := writeChunk(transactionOrder, artists)
		if written.IsErr() {
			return written
		}
		reconcile := []func() result.Result{
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					artistAliasRelation,
					deleteStale,
					rootIDs,
					aliases,
					func(item *model.ArtistAlias) int32 { return item.ArtistID },
					func(item *model.ArtistAlias) int32 { return item.AliasID },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					artistGroupRelation,
					deleteStale,
					rootIDs,
					groups,
					func(item *model.ArtistGroup) int32 { return item.ArtistID },
					func(item *model.ArtistGroup) int32 { return item.GroupID },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					artistMemberRelation,
					deleteStale,
					rootIDs,
					members,
					func(item *model.ArtistMember) int32 { return item.ArtistID },
					func(item *model.ArtistMember) int32 { return item.MemberID },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					artistNameVariationRelation,
					deleteStale,
					rootIDs,
					nameVariations,
					func(item *model.ArtistNameVariation) int32 { return item.ArtistID },
					func(item *model.ArtistNameVariation) int32 { return item.Hash },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					artistURLRelation,
					deleteStale,
					rootIDs,
					urls,
					func(item *model.ArtistURL) int32 { return item.ArtistID },
					func(item *model.ArtistURL) int32 { return item.Hash },
				)
			},
		}
		for _, reconcileRelation := range reconcile {
			written = written.Sum(reconcileRelation())
			if written.IsErr() {
				return written
			}
		}
		return written
	})
}
