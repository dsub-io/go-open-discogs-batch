package batch

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/go-open-discogs-batch/src/unique"
	"github.com/dsub-io/open-discogs-model/model"
)

var (
	labelReleaseItemRelation = integerNullableTextKeyRelation{
		table:             "label_release_item",
		parentColumn:      "release_item_id",
		integerKeyColumn:  "label_id",
		nullableKeyColumn: "category_notation",
	}
	releaseArtistRelation = integerRelation{
		table: "release_item_artist", parentColumn: "release_item_id", keyColumn: "artist_id",
	}
	releaseCreditedArtistRelation = twoIntegerKeyRelation{
		table: "release_item_credited_artist", parentColumn: "release_item_id",
		firstKeyColumn: "artist_id", secondKeyColumn: "hash",
	}
	releaseFormatRelation = integerRelation{
		table: "release_item_format", parentColumn: "release_item_id", keyColumn: "hash",
	}
	releaseGenreRelation = textRelation{
		table: "release_item_genre", parentColumn: "release_item_id", keyColumn: "genre",
	}
	releaseIdentifierRelation = integerRelation{
		table: "release_item_identifier", parentColumn: "release_item_id", keyColumn: "hash",
	}
	releaseStyleRelation = textRelation{
		table: "release_item_style", parentColumn: "release_item_id", keyColumn: "style",
	}
	releaseTrackRelation = integerRelation{
		table: "release_item_track", parentColumn: "release_item_id", keyColumn: "hash",
	}
	releaseVideoRelation = integerRelation{
		table: "release_item_video", parentColumn: "release_item_id", keyColumn: "hash",
	}
	releaseWorkRelation = twoIntegerKeyRelation{
		table: "release_item_work", parentColumn: "release_item_id",
		firstKeyColumn: "label_id", secondKeyColumn: "hash",
	}
)

// TODO: double check ptr exceptions on reference

// GetReleaseStep returns a set of steps in a form of composite notary.
// This is a convenient func such that reduces code and adds syntactic sugar, but nothing more.
func GetReleaseStep(order Order) Step {
	return func() result.Result {
		return insertReleases(order)
	}
}

func insertReleases(order Order) result.Result {
	deleteStale, err := relationTablesContainRows(
		order,
		labelReleaseItemRelation.table,
		releaseArtistRelation.table,
		releaseCreditedArtistRelation.table,
		releaseFormatRelation.table,
		releaseGenreRelation.table,
		releaseIdentifierRelation.table,
		releaseStyleRelation.table,
		releaseTrackRelation.table,
		releaseVideoRelation.table,
		releaseWorkRelation.table,
	)
	if err != nil {
		return result.NewResult(0, err)
	}
	return processRelationChunks(
		order,
		"release relations",
		"release",
		"source-read release relations",
		func(order Order, chunk ChunkMetadata, items []*XmlReleaseRelation) result.Result {
			return writeReleaseRelationChunk(order, chunk, items, deleteStale)
		},
	)
}

func writeReleaseRelationChunk(
	order Order,
	chunk ChunkMetadata,
	items []*XmlReleaseRelation,
	deleteStale bool,
) result.Result {
	return executeChunk(order, chunk, func(transactionOrder Order) result.Result {
		genres := make([]*model.Genre, 0)
		styles := make([]*model.Style, 0)
		rootIDs := make([]int32, 0, len(items))
		releases := make([]*model.ReleaseItem, 0, len(items))
		artists := make([]*model.ReleaseItemArtist, 0)
		creditedArtists := make([]*model.ReleaseItemCreditedArtist, 0)
		works := make([]*model.ReleaseItemWork, 0)
		formats := make([]*model.ReleaseItemFormat, 0)
		releaseStyles := make([]*model.ReleaseItemStyle, 0)
		releaseGenres := make([]*model.ReleaseItemGenre, 0)
		identifiers := make([]*model.ReleaseItemIdentifier, 0)
		tracks := make([]*model.ReleaseItemTrack, 0)
		videos := make([]*model.ReleaseItemVideo, 0)
		labels := make([]*model.LabelReleaseItem, 0)
		for _, item := range items {
			if item == nil {
				continue
			}
			rootIDs = append(rootIDs, item.ID)
			genres = append(genres, item.GetGenres()...)
			styles = append(styles, item.GetStyles()...)
			releases = append(releases, item.GetRelease())
			artists = append(artists, item.GetReleaseArtists()...)
			releaseGenres = append(releaseGenres, item.GetReleaseGenres()...)
			releaseStyles = append(releaseStyles, item.GetReleaseStyles()...)
			works = append(works, item.GetWorks()...)
			labels = append(labels, item.GetLabels()...)
			formats = append(formats, item.GetFormats()...)
			identifiers = append(identifiers, item.GetIdentifiers()...)
			tracks = append(tracks, item.GetTracks()...)
			videos = append(videos, item.GetVideos()...)
			creditedArtists = append(creditedArtists, item.GetCreditedArtists()...)
		}
		var (
			artistError         error
			creditedArtistError error
			workError           error
			styleError          error
			genreError          error
			labelError          error
			formatError         error
			identifierError     error
			trackError          error
			videoError          error
		)
		artists, artistError = deduplicateReleaseArtists(artists)
		creditedArtists, creditedArtistError = deduplicateReleaseCreditedArtists(creditedArtists)
		works, workError = deduplicateReleaseWorks(works)
		releaseStyles, styleError = deduplicateReleaseStyles(releaseStyles)
		releaseGenres, genreError = deduplicateReleaseGenres(releaseGenres)
		labels, labelError = deduplicateLabelReleaseItems(labels)
		formats, formatError = deduplicateReleaseFormats(formats)
		identifiers, identifierError = deduplicateReleaseIdentifiers(identifiers)
		tracks, trackError = deduplicateReleaseTracks(tracks)
		videos, videoError = deduplicateReleaseVideos(videos)
		if deduplicateError := errors.Join(
			artistError,
			creditedArtistError,
			workError,
			styleError,
			genreError,
			labelError,
			formatError,
			identifierError,
			trackError,
			videoError,
		); deduplicateError != nil {
			return result.NewResult(0, deduplicateError)
		}
		rootIDs = unique.Slice(rootIDs)
		if referenceResult := writeReferenceEntities(
			transactionOrder,
			filterGenres(genres),
			filterStyles(styles),
		); referenceResult.IsErr() {
			return referenceResult
		}
		written := writeChunk(transactionOrder, releases)
		if written.IsErr() {
			return written
		}
		reconcile := []func() result.Result{
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					releaseArtistRelation,
					deleteStale,
					rootIDs,
					artists,
					func(item *model.ReleaseItemArtist) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemArtist) int32 { return item.ArtistID },
				)
			},
			func() result.Result {
				return reconcileTwoIntegerKeyRelation(
					transactionOrder,
					releaseCreditedArtistRelation,
					deleteStale,
					rootIDs,
					creditedArtists,
					func(item *model.ReleaseItemCreditedArtist) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemCreditedArtist) int32 { return item.ArtistID },
					func(item *model.ReleaseItemCreditedArtist) int32 { return item.Hash },
				)
			},
			func() result.Result {
				return reconcileTwoIntegerKeyRelation(
					transactionOrder,
					releaseWorkRelation,
					deleteStale,
					rootIDs,
					works,
					func(item *model.ReleaseItemWork) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemWork) int32 { return item.LabelID },
					func(item *model.ReleaseItemWork) int32 { return item.Hash },
				)
			},
			func() result.Result {
				return reconcileTextRelation(
					transactionOrder,
					releaseStyleRelation,
					deleteStale,
					rootIDs,
					releaseStyles,
					func(item *model.ReleaseItemStyle) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemStyle) string { return item.Style },
				)
			},
			func() result.Result {
				return reconcileTextRelation(
					transactionOrder,
					releaseGenreRelation,
					deleteStale,
					rootIDs,
					releaseGenres,
					func(item *model.ReleaseItemGenre) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemGenre) string { return item.Genre },
				)
			},
			func() result.Result {
				return reconcileIntegerNullableTextKeyRelation(
					transactionOrder,
					labelReleaseItemRelation,
					deleteStale,
					rootIDs,
					labels,
					func(item *model.LabelReleaseItem) int32 { return item.ReleaseItemID },
					func(item *model.LabelReleaseItem) int32 { return item.LabelID },
					func(item *model.LabelReleaseItem) *string { return item.CategoryNotation },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					releaseFormatRelation,
					deleteStale,
					rootIDs,
					formats,
					func(item *model.ReleaseItemFormat) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemFormat) int32 { return item.Hash },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					releaseIdentifierRelation,
					deleteStale,
					rootIDs,
					identifiers,
					func(item *model.ReleaseItemIdentifier) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemIdentifier) int32 { return item.Hash },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					releaseTrackRelation,
					deleteStale,
					rootIDs,
					tracks,
					func(item *model.ReleaseItemTrack) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemTrack) int32 { return item.Hash },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					releaseVideoRelation,
					deleteStale,
					rootIDs,
					videos,
					func(item *model.ReleaseItemVideo) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemVideo) int32 { return item.Hash },
				)
			},
		}
		for _, reconcileRelation := range reconcile {
			written = written.Sum(reconcileRelation())
			if written.IsErr() {
				return written
			}
		}
		return written.Sum(updateMasterMainReleases(transactionOrder, items))
	})
}

func updateMasterMainReleases(order Order, releases []*XmlReleaseRelation) result.Result {
	updates := make(map[int32]int32)
	releaseIDs := make([]int32, 0, len(releases))
	for _, release := range releases {
		if release == nil {
			continue
		}
		releaseIDs = append(releaseIDs, release.ID)
		if !release.MasterInfo.IsMaster || release.MasterInfo.MasterID == nil {
			continue
		}
		updates[*release.MasterInfo.MasterID] = release.ID
	}
	releaseIDs = unique.Slice(releaseIDs)
	mainReleaseIDs := make([]int32, 0, len(updates))
	for _, releaseID := range updates {
		mainReleaseIDs = append(mainReleaseIDs, releaseID)
	}
	cleared := order.getDB().Exec(
		`update master
		    set main_release_id = null,
		        last_modified_at = ?
		  where main_release_id = any(?::integer[])
		    and not (main_release_id = any(?::integer[]))`,
		time.Now().UTC(),
		postgresArray(releaseIDs),
		postgresArray(mainReleaseIDs),
	)
	if cleared.Error != nil {
		return result.NewResult(0, cleared.Error)
	}
	if len(updates) == 0 {
		return result.NewResult(int(cleared.RowsAffected), nil)
	}

	masterIDs := make([]int32, 0, len(updates))
	for masterID := range updates {
		masterIDs = append(masterIDs, masterID)
	}
	sort.Slice(masterIDs, func(left, right int) bool { return masterIDs[left] < masterIDs[right] })

	updated := int(cleared.RowsAffected)
	batchSize := min(order.getChunkSize(), 32_767)
	for start := 0; start < len(masterIDs); start += batchSize {
		end := min(start+batchSize, len(masterIDs))
		query, arguments := masterMainReleaseUpdateStatement(masterIDs[start:end], updates)
		tx := order.getDB().Exec(query, arguments...)
		if tx.Error != nil {
			return result.NewResult(updated, tx.Error)
		}
		updated += int(tx.RowsAffected)
	}
	return result.NewResult(updated, nil)
}

func masterMainReleaseUpdateStatement(
	masterIDs []int32,
	updates map[int32]int32,
) (string, []any) {
	rows := make([]string, len(masterIDs))
	arguments := make([]any, 0, 1+len(masterIDs)*2)
	arguments = append(arguments, time.Now().UTC())
	for index, masterID := range masterIDs {
		rows[index] = "(?::integer, ?::integer)"
		arguments = append(arguments, masterID, updates[masterID])
	}
	return `UPDATE master AS target
		SET main_release_id = incoming.release_id,
			last_modified_at = ?
		FROM (VALUES ` + strings.Join(rows, ", ") + `) AS incoming(master_id, release_id)
		WHERE target.id = incoming.master_id
			AND target.main_release_id IS DISTINCT FROM incoming.release_id`, arguments
}

func filterGenres(genres []*model.Genre) []*model.Genre {
	r := make([]*model.Genre, 0)
	for _, v := range unique.Slice(genres) {
		if name := strings.TrimSpace(v.Name); len(name) == 0 {
			continue
		}
		r = append(r, v)
	}
	return r
}

func filterStyles(styles []*model.Style) []*model.Style {
	r := make([]*model.Style, 0)
	for _, v := range unique.Slice(styles) {
		if name := strings.TrimSpace(v.Name); len(name) == 0 {
			continue
		}
		r = append(r, v)
	}
	return r
}
