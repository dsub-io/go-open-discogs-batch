package batch

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
)

const (
	releaseIdentityColumn = "identity_sha256"
	releaseMasterLockSQL  = `with candidate_master_ids as (
	    select unnest(?::integer[]) as id
	    union
	    select current.id
	      from master as current
	     where current.main_release_id = any(?::integer[])
	)
	select target.id
	  from master as target
	  join candidate_master_ids as candidate
	    on candidate.id = target.id
	 order by target.id
	 for update of target`
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
	releaseCreditedArtistRelation = digestTwoIntegerKeyRelation{
		table: "release_item_credited_artist", parentColumn: "release_item_id",
		firstKeyColumn: "artist_id", secondKeyColumn: "hash", identityColumn: releaseIdentityColumn,
	}
	releaseFormatRelation = digestIntegerRelation{
		table: "release_item_format", parentColumn: "release_item_id",
		keyColumn: "hash", identityColumn: releaseIdentityColumn,
	}
	releaseGenreRelation = textRelation{
		table: "release_item_genre", parentColumn: "release_item_id", keyColumn: "genre",
	}
	releaseIdentifierRelation = digestIntegerRelation{
		table: "release_item_identifier", parentColumn: "release_item_id",
		keyColumn: "hash", identityColumn: releaseIdentityColumn,
	}
	releaseStyleRelation = textRelation{
		table: "release_item_style", parentColumn: "release_item_id", keyColumn: "style",
	}
	releaseTrackRelation = digestIntegerRelation{
		table: "release_item_track", parentColumn: "release_item_id",
		keyColumn: "hash", identityColumn: releaseIdentityColumn,
	}
	releaseVideoRelation = digestIntegerRelation{
		table: "release_item_video", parentColumn: "release_item_id",
		keyColumn: "hash", identityColumn: releaseIdentityColumn,
	}
	releaseWorkRelation = digestTwoIntegerKeyRelation{
		table: "release_item_work", parentColumn: "release_item_id",
		firstKeyColumn: "label_id", secondKeyColumn: "hash", identityColumn: releaseIdentityColumn,
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
	return processRelationChunks(
		order,
		"release relations",
		"release",
		"source-read release relations",
		func(order Order, chunk ChunkMetadata, items []*XmlReleaseRelation) result.Result {
			return writeReleaseRelationChunk(order, chunk, items)
		},
	)
}

func writeReleaseRelationChunk(
	order Order,
	chunk ChunkMetadata,
	items []*XmlReleaseRelation,
) result.Result {
	observedAt := time.Now().UTC()
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
		item.observedAt = observedAt
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
		releaseError        error
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
	releases, releaseError = deduplicateReleaseItems(releases)
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
		releaseError,
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
	rootIDs = deduplicateComparable(rootIDs)
	genres = filterGenres(genres)
	styles = filterStyles(styles)
	sortReferenceEntities(genres, styles)
	genres, styles = filterConfirmedReferenceEntities(genres, styles)
	written := executeChunk(order, chunk, func(transactionOrder Order) result.Result {
		existingRoots, existingRootsError := findExistingRelationRoots(
			transactionOrder,
			rootIDs,
			relationRootTable{releaseArtistRelation.table, releaseArtistRelation.parentColumn},
			relationRootTable{releaseCreditedArtistRelation.table, releaseCreditedArtistRelation.parentColumn},
			relationRootTable{releaseWorkRelation.table, releaseWorkRelation.parentColumn},
			relationRootTable{releaseStyleRelation.table, releaseStyleRelation.parentColumn},
			relationRootTable{releaseGenreRelation.table, releaseGenreRelation.parentColumn},
			relationRootTable{labelReleaseItemRelation.table, labelReleaseItemRelation.parentColumn},
			relationRootTable{releaseFormatRelation.table, releaseFormatRelation.parentColumn},
			relationRootTable{releaseIdentifierRelation.table, releaseIdentifierRelation.parentColumn},
			relationRootTable{releaseTrackRelation.table, releaseTrackRelation.parentColumn},
			relationRootTable{releaseVideoRelation.table, releaseVideoRelation.parentColumn},
		)
		if existingRootsError != nil {
			return result.NewResult(0, existingRootsError)
		}
		if lockError := lockReleaseMasterRows(transactionOrder, rootIDs, items); lockError != nil {
			return result.NewResult(0, lockError)
		}
		if referenceResult := writeReferenceEntities(
			transactionOrder,
			genres,
			styles,
		); referenceResult.IsErr() {
			return referenceResult
		}
		written := doWrite(releases, transactionOrder.getChunkSize(), transactionOrder.getDB())
		if written.IsErr() {
			return written
		}
		reconcile := []func() result.Result{
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					releaseArtistRelation,
					len(existingRoots.forTable(releaseArtistRelation.table)) > 0,
					existingRoots.forTable(releaseArtistRelation.table),
					artists,
					func(item *model.ReleaseItemArtist) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemArtist) int32 { return item.ArtistID },
				)
			},
			func() result.Result {
				return reconcileDigestTwoIntegerKeyRelation(
					transactionOrder,
					releaseCreditedArtistRelation,
					len(existingRoots.forTable(releaseCreditedArtistRelation.table)) > 0,
					existingRoots.forTable(releaseCreditedArtistRelation.table),
					creditedArtists,
					func(item *model.ReleaseItemCreditedArtist) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemCreditedArtist) int32 { return item.ArtistID },
					func(item *model.ReleaseItemCreditedArtist) int32 { return item.Hash },
					func(item *model.ReleaseItemCreditedArtist) []byte { return item.IdentitySHA256 },
				)
			},
			func() result.Result {
				return reconcileDigestTwoIntegerKeyRelation(
					transactionOrder,
					releaseWorkRelation,
					len(existingRoots.forTable(releaseWorkRelation.table)) > 0,
					existingRoots.forTable(releaseWorkRelation.table),
					works,
					func(item *model.ReleaseItemWork) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemWork) int32 { return item.LabelID },
					func(item *model.ReleaseItemWork) int32 { return item.Hash },
					func(item *model.ReleaseItemWork) []byte { return item.IdentitySHA256 },
				)
			},
			func() result.Result {
				return reconcileTextRelation(
					transactionOrder,
					releaseStyleRelation,
					len(existingRoots.forTable(releaseStyleRelation.table)) > 0,
					existingRoots.forTable(releaseStyleRelation.table),
					releaseStyles,
					func(item *model.ReleaseItemStyle) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemStyle) string { return item.Style },
				)
			},
			func() result.Result {
				return reconcileTextRelation(
					transactionOrder,
					releaseGenreRelation,
					len(existingRoots.forTable(releaseGenreRelation.table)) > 0,
					existingRoots.forTable(releaseGenreRelation.table),
					releaseGenres,
					func(item *model.ReleaseItemGenre) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemGenre) string { return item.Genre },
				)
			},
			func() result.Result {
				return reconcileIntegerNullableTextKeyRelation(
					transactionOrder,
					labelReleaseItemRelation,
					len(existingRoots.forTable(labelReleaseItemRelation.table)) > 0,
					existingRoots.forTable(labelReleaseItemRelation.table),
					labels,
					func(item *model.LabelReleaseItem) int32 { return item.ReleaseItemID },
					func(item *model.LabelReleaseItem) int32 { return item.LabelID },
					func(item *model.LabelReleaseItem) *string { return item.CategoryNotation },
				)
			},
			func() result.Result {
				return reconcileDigestIntegerRelation(
					transactionOrder,
					releaseFormatRelation,
					len(existingRoots.forTable(releaseFormatRelation.table)) > 0,
					existingRoots.forTable(releaseFormatRelation.table),
					formats,
					func(item *model.ReleaseItemFormat) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemFormat) int32 { return item.Hash },
					func(item *model.ReleaseItemFormat) []byte { return item.IdentitySHA256 },
				)
			},
			func() result.Result {
				return reconcileDigestIntegerRelation(
					transactionOrder,
					releaseIdentifierRelation,
					len(existingRoots.forTable(releaseIdentifierRelation.table)) > 0,
					existingRoots.forTable(releaseIdentifierRelation.table),
					identifiers,
					func(item *model.ReleaseItemIdentifier) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemIdentifier) int32 { return item.Hash },
					func(item *model.ReleaseItemIdentifier) []byte { return item.IdentitySHA256 },
				)
			},
			func() result.Result {
				return reconcileDigestIntegerRelation(
					transactionOrder,
					releaseTrackRelation,
					len(existingRoots.forTable(releaseTrackRelation.table)) > 0,
					existingRoots.forTable(releaseTrackRelation.table),
					tracks,
					func(item *model.ReleaseItemTrack) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemTrack) int32 { return item.Hash },
					func(item *model.ReleaseItemTrack) []byte { return item.IdentitySHA256 },
				)
			},
			func() result.Result {
				return reconcileDigestIntegerRelation(
					transactionOrder,
					releaseVideoRelation,
					len(existingRoots.forTable(releaseVideoRelation.table)) > 0,
					existingRoots.forTable(releaseVideoRelation.table),
					videos,
					func(item *model.ReleaseItemVideo) int32 { return item.ReleaseItemID },
					func(item *model.ReleaseItemVideo) int32 { return item.Hash },
					func(item *model.ReleaseItemVideo) []byte { return item.IdentitySHA256 },
				)
			},
		}
		for _, reconcileRelation := range reconcile {
			written = written.Sum(reconcileRelation())
			if written.IsErr() {
				return written
			}
		}
		return written.Sum(updateMasterMainReleases(transactionOrder, items, observedAt))
	})
	if !written.IsErr() {
		confirmReferenceEntities(genres, styles)
	}
	return written
}

func lockReleaseMasterRows(
	order Order,
	releaseIDs []int32,
	releases []*XmlReleaseRelation,
) error {
	if len(releaseIDs) == 0 {
		return nil
	}
	masterIDs := releaseMasterIDsToLock(releases)

	type lockedMaster struct {
		ID int32
	}
	locked := make([]lockedMaster, 0, len(masterIDs))
	query := order.getDB().Raw(
		releaseMasterLockSQL,
		postgresArray(masterIDs),
		postgresArray(releaseIDs),
	).Scan(&locked)
	if query.Error != nil {
		return fmt.Errorf("lock release master rows: %w", query.Error)
	}
	return nil
}

func releaseMasterIDsToLock(releases []*XmlReleaseRelation) []int32 {
	masterIDs := make([]int32, 0, len(releases))
	for _, release := range releases {
		if release == nil || !release.MasterInfo.IsMaster || release.MasterInfo.MasterID == nil {
			continue
		}
		masterIDs = append(masterIDs, *release.MasterInfo.MasterID)
	}
	masterIDs = deduplicateComparable(masterIDs)
	sort.Slice(masterIDs, func(left, right int) bool { return masterIDs[left] < masterIDs[right] })
	return masterIDs
}

func updateMasterMainReleases(
	order Order,
	releases []*XmlReleaseRelation,
	observedAt time.Time,
) result.Result {
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
	releaseIDs = deduplicateComparable(releaseIDs)
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
		observedAt,
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
	query, arguments := masterMainReleaseUpdateStatement(masterIDs, updates, observedAt)
	tx := order.getDB().Exec(query, arguments...)
	if tx.Error != nil {
		return result.NewResult(updated, tx.Error)
	}
	updated += int(tx.RowsAffected)
	return result.NewResult(updated, nil)
}

func masterMainReleaseUpdateStatement(
	masterIDs []int32,
	updates map[int32]int32,
	observedAt time.Time,
) (string, []any) {
	releaseIDs := make([]int32, len(masterIDs))
	for index, masterID := range masterIDs {
		releaseIDs[index] = updates[masterID]
	}
	return `UPDATE master AS target
		SET main_release_id = incoming.release_id,
			last_modified_at = ?
		FROM unnest(?::integer[], ?::integer[]) AS incoming(master_id, release_id)
		WHERE target.id = incoming.master_id
			AND target.main_release_id IS DISTINCT FROM incoming.release_id`, []any{
			observedAt,
			postgresArray(masterIDs),
			postgresArray(releaseIDs),
		}
}

func filterGenres(genres []*model.Genre) []*model.Genre {
	seen := make(map[string]struct{}, len(genres))
	r := make([]*model.Genre, 0, len(genres))
	for _, value := range genres {
		if value == nil {
			continue
		}
		name := strings.TrimSpace(value.Name)
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		value.Name = name
		r = append(r, value)
	}
	return r
}

func filterStyles(styles []*model.Style) []*model.Style {
	seen := make(map[string]struct{}, len(styles))
	r := make([]*model.Style, 0, len(styles))
	for _, value := range styles {
		if value == nil {
			continue
		}
		name := strings.TrimSpace(value.Name)
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		value.Name = name
		r = append(r, value)
	}
	return r
}
