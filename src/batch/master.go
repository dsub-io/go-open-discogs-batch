package batch

import (
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/go-open-discogs-batch/src/unique"
	"github.com/dsub-io/open-discogs-model/model"
)

var (
	masterArtistRelation = integerRelation{
		table: "master_artist", parentColumn: "master_id", keyColumn: "artist_id",
	}
	masterGenreRelation = textRelation{
		table: "master_genre", parentColumn: "master_id", keyColumn: "genre",
	}
	masterStyleRelation = textRelation{
		table: "master_style", parentColumn: "master_id", keyColumn: "style",
	}
	masterVideoRelation = integerRelation{
		table: "master_video", parentColumn: "master_id", keyColumn: "hash",
	}
)

//TODO: add master release step for future use

func GetMasterStep(order Order) Step {
	return func() result.Result {
		return InsertMasterRelations(order)
	}
}

func InsertMasterRelations(order Order) result.Result {
	deleteStale, err := relationTablesContainRows(
		order,
		masterArtistRelation.table,
		masterGenreRelation.table,
		masterStyleRelation.table,
		masterVideoRelation.table,
	)
	if err != nil {
		return result.NewResult(0, err)
	}
	return processRelationChunks(
		order,
		"master relations",
		"master",
		"updating master relations...",
		func(order Order, chunk ChunkMetadata, items []*XmlMasterRelation) result.Result {
			return writeMasterRelationChunk(order, chunk, items, deleteStale)
		},
	)
}

func writeMasterRelationChunk(
	order Order,
	chunk ChunkMetadata,
	items []*XmlMasterRelation,
	deleteStale bool,
) result.Result {
	for _, item := range items {
		if item == nil {
			continue
		}
		cache.MasterIDs.Add(item.ID)
	}
	return executeChunk(order, chunk, func(transactionOrder Order) result.Result {
		styles := make([]*model.Style, 0)
		genres := make([]*model.Genre, 0)
		rootIDs := make([]int32, 0, len(items))
		masters := make([]*model.Master, 0, len(items))
		videos := make([]*model.MasterVideo, 0)
		masterStyles := make([]*model.MasterStyle, 0)
		masterGenres := make([]*model.MasterGenre, 0)
		artists := make([]*model.MasterArtist, 0)
		for _, item := range items {
			if item == nil {
				continue
			}
			rootIDs = append(rootIDs, item.ID)
			genres = append(genres, item.GetGenres()...)
			styles = append(styles, item.GetStyles()...)
			masters = append(masters, item.GetMaster())
			masterStyles = append(masterStyles, item.GetMasterStyles()...)
			masterGenres = append(masterGenres, item.GetMasterGenres()...)
			videos = append(videos, item.GetMasterVideos()...)
			artists = append(artists, item.GetMasterArtists()...)
		}
		rootIDs = unique.Slice(rootIDs)
		if referenceResult := writeReferenceEntities(
			transactionOrder,
			filterGenres(genres),
			filterStyles(styles),
		); referenceResult.IsErr() {
			return referenceResult
		}
		written := writeChunk(transactionOrder, masters)
		if written.IsErr() {
			return written
		}
		reconcile := []func() result.Result{
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					masterArtistRelation,
					deleteStale,
					rootIDs,
					artists,
					func(item *model.MasterArtist) int32 { return item.MasterID },
					func(item *model.MasterArtist) int32 { return item.ArtistID },
				)
			},
			func() result.Result {
				return reconcileTextRelation(
					transactionOrder,
					masterGenreRelation,
					deleteStale,
					rootIDs,
					masterGenres,
					func(item *model.MasterGenre) int32 { return item.MasterID },
					func(item *model.MasterGenre) string { return item.Genre },
				)
			},
			func() result.Result {
				return reconcileTextRelation(
					transactionOrder,
					masterStyleRelation,
					deleteStale,
					rootIDs,
					masterStyles,
					func(item *model.MasterStyle) int32 { return item.MasterID },
					func(item *model.MasterStyle) string { return item.Style },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					masterVideoRelation,
					deleteStale,
					rootIDs,
					videos,
					func(item *model.MasterVideo) int32 { return item.MasterID },
					func(item *model.MasterVideo) int32 { return item.Hash },
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
