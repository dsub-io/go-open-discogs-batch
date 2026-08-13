package batch

import (
	"errors"

	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
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
	return processRelationChunks(
		order,
		"master relations",
		"master",
		"source-read master relations",
		func(order Order, chunk ChunkMetadata, items []*XmlMasterRelation) result.Result {
			return writeMasterRelationChunk(order, chunk, items)
		},
	)
}

func writeMasterRelationChunk(
	order Order,
	chunk ChunkMetadata,
	items []*XmlMasterRelation,
) result.Result {
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
		cache.MasterIDs.Add(item.ID)
		rootIDs = append(rootIDs, item.ID)
		genres = append(genres, item.GetGenres()...)
		styles = append(styles, item.GetStyles()...)
		masters = append(masters, item.GetMaster())
		masterStyles = append(masterStyles, item.GetMasterStyles()...)
		masterGenres = append(masterGenres, item.GetMasterGenres()...)
		videos = append(videos, item.GetMasterVideos()...)
		artists = append(artists, item.GetMasterArtists()...)
	}
	var (
		masterError error
		artistError error
		genreError  error
		styleError  error
		videoError  error
	)
	masters, masterError = deduplicateMasters(masters)
	artists, artistError = deduplicateMasterArtists(artists)
	masterGenres, genreError = deduplicateMasterGenres(masterGenres)
	masterStyles, styleError = deduplicateMasterStyles(masterStyles)
	videos, videoError = deduplicateMasterVideos(videos)
	if deduplicateError := errors.Join(
		masterError,
		artistError,
		genreError,
		styleError,
		videoError,
	); deduplicateError != nil {
		return result.NewResult(0, deduplicateError)
	}
	rootIDs = deduplicateComparable(rootIDs)
	genres = filterGenres(genres)
	styles = filterStyles(styles)
	sortReferenceEntities(genres, styles)
	genres, styles = filterConfirmedReferenceEntities(genres, styles)
	written := executeChunk(order, chunk, func(transactionOrder Order) result.Result {
		existingRoots, err := findExistingRelationRoots(
			transactionOrder,
			rootIDs,
			relationRootTable{masterArtistRelation.table, masterArtistRelation.parentColumn},
			relationRootTable{masterGenreRelation.table, masterGenreRelation.parentColumn},
			relationRootTable{masterStyleRelation.table, masterStyleRelation.parentColumn},
			relationRootTable{masterVideoRelation.table, masterVideoRelation.parentColumn},
		)
		if err != nil {
			return result.NewResult(0, err)
		}
		if referenceResult := writeReferenceEntities(
			transactionOrder,
			genres,
			styles,
		); referenceResult.IsErr() {
			return referenceResult
		}
		written := doWrite(masters, transactionOrder.getChunkSize(), transactionOrder.getDB())
		if written.IsErr() {
			return written
		}
		reconcile := []func() result.Result{
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					masterArtistRelation,
					len(existingRoots.forTable(masterArtistRelation.table)) > 0,
					existingRoots.forTable(masterArtistRelation.table),
					artists,
					func(item *model.MasterArtist) int32 { return item.MasterID },
					func(item *model.MasterArtist) int32 { return item.ArtistID },
				)
			},
			func() result.Result {
				return reconcileTextRelation(
					transactionOrder,
					masterGenreRelation,
					len(existingRoots.forTable(masterGenreRelation.table)) > 0,
					existingRoots.forTable(masterGenreRelation.table),
					masterGenres,
					func(item *model.MasterGenre) int32 { return item.MasterID },
					func(item *model.MasterGenre) string { return item.Genre },
				)
			},
			func() result.Result {
				return reconcileTextRelation(
					transactionOrder,
					masterStyleRelation,
					len(existingRoots.forTable(masterStyleRelation.table)) > 0,
					existingRoots.forTable(masterStyleRelation.table),
					masterStyles,
					func(item *model.MasterStyle) int32 { return item.MasterID },
					func(item *model.MasterStyle) string { return item.Style },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					masterVideoRelation,
					len(existingRoots.forTable(masterVideoRelation.table)) > 0,
					existingRoots.forTable(masterVideoRelation.table),
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
	if !written.IsErr() {
		confirmReferenceEntities(genres, styles)
	}
	return written
}
