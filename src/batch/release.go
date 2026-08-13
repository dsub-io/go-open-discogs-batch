package batch

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
	"gorm.io/gorm"
)

const (
	releaseIdentityColumn      = "identity_sha256"
	releaseMainReleaseClearSQL = `WITH desired AS MATERIALIZED (
		SELECT DISTINCT ON (release_item.master_id)
		       release_item.master_id,
		       release_item.id AS release_id
		  FROM release_item
		 WHERE release_item.is_master IS TRUE
		   AND release_item.master_id IS NOT NULL
		 ORDER BY release_item.master_id,
		          release_item.last_modified_at DESC,
		          release_item.id DESC
	),
	stale AS MATERIALIZED (
		SELECT target.id
		  FROM master AS target
		  LEFT JOIN desired ON desired.master_id = target.id
		 WHERE target.main_release_id IS DISTINCT FROM desired.release_id
		   AND target.main_release_id IS NOT NULL
		 ORDER BY target.id
		 FOR UPDATE OF target
	)
	UPDATE master AS target
	   SET main_release_id = NULL
	  FROM stale
	 WHERE target.id = stale.id`
	releaseMainReleaseSetSQL = `WITH desired AS MATERIALIZED (
		SELECT DISTINCT ON (release_item.master_id)
		       release_item.master_id,
		       release_item.id AS release_id
		  FROM release_item
		 WHERE release_item.is_master IS TRUE
		   AND release_item.master_id IS NOT NULL
		 ORDER BY release_item.master_id,
		          release_item.last_modified_at DESC,
		          release_item.id DESC
	),
	pending AS MATERIALIZED (
		SELECT target.id,
		       desired.release_id
		  FROM desired
		  JOIN master AS target ON target.id = desired.master_id
		 WHERE target.main_release_id IS DISTINCT FROM desired.release_id
		 ORDER BY target.id
		 FOR UPDATE OF target
	)
	UPDATE master AS target
	   SET main_release_id = pending.release_id
	  FROM pending
	 WHERE target.id = pending.id`
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
	return processRelationChunksWithFinalizer(
		order,
		"release relations",
		"release",
		"source-read release relations",
		func(order Order, chunk ChunkMetadata, items []*XmlReleaseRelation) result.Result {
			return writeReleaseRelationChunk(order, chunk, items)
		},
		finalizeReleaseImport,
	)
}

func writeReleaseRelationChunk(
	order Order,
	chunk ChunkMetadata,
	items []*XmlReleaseRelation,
) result.Result {
	observedAt := time.Now().UTC()
	assignChunkObservedAt(items, observedAt)
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
					func(item *model.ReleaseItemCreditedArtist) *model.SHA256Digest { return item.IdentitySHA256 },
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
					func(item *model.ReleaseItemWork) *model.SHA256Digest { return item.IdentitySHA256 },
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
					func(item *model.ReleaseItemFormat) *model.SHA256Digest { return item.IdentitySHA256 },
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
					func(item *model.ReleaseItemIdentifier) *model.SHA256Digest { return item.IdentitySHA256 },
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
					func(item *model.ReleaseItemTrack) *model.SHA256Digest { return item.IdentitySHA256 },
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
					func(item *model.ReleaseItemVideo) *model.SHA256Digest { return item.IdentitySHA256 },
				)
			},
		}
		return written.Sum(reconcileRelations(reconcile))
	})
	if !written.IsErr() {
		confirmReferenceEntities(genres, styles)
	}
	return written
}

func finalizeReleaseImport(order Order, totalItems, totalChunks int64) result.Result {
	reconciled := reconcileMasterMainReleases(order)
	if reconciled.IsErr() {
		return reconciled
	}
	return reconciled.Sum(
		result.NewResult(0, completeEntityProgress(order, totalItems, totalChunks)),
	)
}

// Historical roots are retained, so the newest observed main root wins; release ID breaks ties.
func reconcileMasterMainReleases(order Order) result.Result {
	written := result.NewResult(0, nil)
	err := order.getDB().WithContext(order.getContext()).Transaction(func(tx *gorm.DB) error {
		cleared := tx.Exec(releaseMainReleaseClearSQL)
		if cleared.Error != nil {
			return fmt.Errorf("clear stale master main releases: %w", cleared.Error)
		}
		set := tx.Exec(releaseMainReleaseSetSQL)
		if set.Error != nil {
			return fmt.Errorf("set current master main releases: %w", set.Error)
		}
		written = result.NewResult(int(cleared.RowsAffected+set.RowsAffected), nil)
		return nil
	})
	if err != nil {
		return result.NewResult(0, err)
	}
	return written
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
