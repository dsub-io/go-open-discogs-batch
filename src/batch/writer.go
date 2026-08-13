package batch

import (
	"fmt"
	"slices"
	"strings"

	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func writeChunk(order Order, slices ...interface{}) result.Result {
	return NewWriter(order.getDB()).Write(order.getChunkSize(), slices...)
}

func writeReferenceEntities(
	order Order,
	genres []*model.Genre,
	styles []*model.Style,
) result.Result {
	for _, items := range []interface{}{genres, styles} {
		if err := order.getDB().
			Clauses(clause.OnConflict{DoNothing: true}).
			CreateInBatches(items, order.getChunkSize()).Error; err != nil {
			return result.NewResult(0, err)
		}
	}
	return result.NewResult(0, nil)
}

func sortReferenceEntities(genres []*model.Genre, styles []*model.Style) {
	slices.SortFunc(genres, func(left, right *model.Genre) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(styles, func(left, right *model.Style) int {
		return strings.Compare(left.Name, right.Name)
	})
}

func filterConfirmedReferenceEntities(
	genres []*model.Genre,
	styles []*model.Style,
) ([]*model.Genre, []*model.Style) {
	pendingGenres := make([]*model.Genre, 0, len(genres))
	for _, genre := range genres {
		if !cache.GenreNames.Contains(genre.Name) {
			pendingGenres = append(pendingGenres, genre)
		}
	}
	pendingStyles := make([]*model.Style, 0, len(styles))
	for _, style := range styles {
		if !cache.StyleNames.Contains(style.Name) {
			pendingStyles = append(pendingStyles, style)
		}
	}
	return pendingGenres, pendingStyles
}

func confirmReferenceEntities(genres []*model.Genre, styles []*model.Style) {
	for _, genre := range genres {
		cache.GenreNames.Add(genre.Name)
	}
	for _, style := range styles {
		cache.StyleNames.Add(style.Name)
	}
}

type Writer interface {
	Write(chunkSize int, items ...interface{}) result.Result
}

type gormWriter struct {
	db *gorm.DB
}

var NewWriter = newWriter

func newWriter(db *gorm.DB) Writer {
	return &gormWriter{db: db}
}

func (g gormWriter) Write(chunkSize int, slices ...interface{}) result.Result {
	var (
		updated = 0
		err     error
	)
	err = nil
	for _, slice := range slices {
		if err != nil {
			break
		}
		var r result.Result
		switch o := slice.(type) {
		case []*model.Artist:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateArtists)
		case []*model.ArtistURL:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateArtistURLs)
		case []*model.ArtistAlias:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateArtistAliases)
		case []*model.ArtistGroup:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateArtistGroups)
		case []*model.ArtistMember:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateArtistMembers)
		case []*model.ArtistNameVariation:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateArtistNameVariations)
		case []*model.Label:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateLabels)
		case []*model.LabelURL:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateLabelURLs)
		case []*model.LabelSubLabel:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateLabelSubLabels)
		case []*model.LabelReleaseItem:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateLabelReleaseItems)
		case []*model.Master:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateMasters)
		case []*model.MasterArtist:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateMasterArtists)
		case []*model.MasterGenre:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateMasterGenres)
		case []*model.MasterStyle:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateMasterStyles)
		case []*model.MasterVideo:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateMasterVideos)
		case []*model.ReleaseItem:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateReleaseItems)
		case []*model.ReleaseItemArtist:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseArtists)
		case []*model.ReleaseItemWork:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseWorks)
		case []*model.ReleaseItemFormat:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseFormats)
		case []*model.ReleaseItemCreditedArtist:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseCreditedArtists)
		case []*model.ReleaseItemGenre:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseGenres)
		case []*model.ReleaseItemStyle:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseStyles)
		case []*model.ReleaseItemIdentifier:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseIdentifiers)
		case []*model.ReleaseItemImage:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseImages)
		case []*model.ReleaseItemTrack:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseTracks)
		case []*model.ReleaseItemVideo:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseVideos)
		case []*model.Style:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateStyles)
		case []*model.Genre:
			r = writeCanonicalBatch(o, chunkSize, g.db, deduplicateGenres)
		}
		if r != nil {
			updated += r.Count()
			err = r.Err()
		}
	}

	return result.NewResult(updated, err)
}

func writeCanonicalBatch[T any](
	items []T,
	chunkSize int,
	db *gorm.DB,
	deduplicate func([]T) ([]T, error),
) result.Result {
	deduplicated, err := deduplicate(items)
	if err != nil {
		return result.NewResult(0, err)
	}
	return doWrite(deduplicated, chunkSize, db)
}

func writeReleaseRelationBatch[T any](
	items []T,
	chunkSize int,
	db *gorm.DB,
	deduplicate func([]T) ([]T, error),
) result.Result {
	deduplicated, err := deduplicate(items)
	if err != nil {
		return result.NewResult(0, err)
	}
	return doWrite(deduplicated, chunkSize, db)
}

func doWrite[T any](items []T, chunkSize int, db *gorm.DB) result.Result {
	if len(items) == 0 {
		return result.NewResult(0, nil)
	}
	statement := &gorm.Statement{DB: db}
	if err := statement.Parse(items[0]); err != nil {
		return result.NewResult(0, fmt.Errorf("parse insert schema: %w", err))
	}
	chunkSize, err := postgresSafeBatchSize(chunkSize, len(statement.Schema.DBNames))
	if err != nil {
		return result.NewResult(0, err)
	}
	var (
		start     = 0
		end       = chunkSize
		resultSum = result.NewResult(0, nil)
		size      = len(items)
		cl        = ExtractClause(items[0])
	)
	for {
		if resultSum.IsErr() {
			logrus.Errorf("error during insertion: %+v\n", resultSum.Err())
		}
		if start >= size || resultSum.Err() != nil {
			return resultSum
		}
		if end > size {
			end = size
		}
		part := items[start:end]
		tx := db.Clauses(cl).CreateInBatches(&part, len(part))
		resultSum = resultSum.Sum(result.NewResult(int(tx.RowsAffected), tx.Error))
		start += chunkSize
		end += chunkSize
	}
}

func postgresSafeBatchSize(requested, columnCount int) (int, error) {
	const postgresBindParameterLimit = 65_535
	if requested < 1 {
		return 0, fmt.Errorf("chunk size must be positive")
	}
	if columnCount < 1 {
		return 0, fmt.Errorf("insert schema has no columns")
	}
	maxRows := postgresBindParameterLimit / columnCount
	if maxRows < 1 {
		return 0, fmt.Errorf("insert schema has too many columns: %d", columnCount)
	}
	return min(requested, maxRows), nil
}
