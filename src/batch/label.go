package batch

import (
	"errors"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
)

var (
	labelSubLabelRelation = integerRelation{
		table: "label_sub_label", parentColumn: "parent_label_id", keyColumn: "sub_label_id",
	}
	labelURLRelation = integerRelation{
		table: "label_url", parentColumn: "label_id", keyColumn: "hash",
	}
)

//TODO: add label release step for future use

func GetLabelStep(order Order) Step {
	return func() result.Result {
		updated := 0
		res := insertLabels(order)
		updated += res.Count()
		if res.IsErr() {
			return result.NewResult(updated, res.Err())
		}
		res = insertLabelRelations(order)
		updated += res.Count()
		if res.IsErr() {
			return result.NewResult(updated, res.Err())
		}
		return result.NewResult(updated, nil)
	}
}

func insertLabels(order Order) result.Result {
	return InsertSimple(order, "labels", "label", (*XmlLabel).TransformAt)
}

func insertLabelRelations(order Order) result.Result {
	return processRelationChunks(
		order,
		"label relations",
		"label",
		"source-read label relations",
		func(order Order, chunk ChunkMetadata, items []*XmlLabelRelation) result.Result {
			return writeLabelRelationChunk(order, chunk, items)
		},
	)
}

func writeLabelRelationChunk(
	order Order,
	chunk ChunkMetadata,
	items []*XmlLabelRelation,
) result.Result {
	observedAt := time.Now().UTC()
	rootIDs := make([]int32, 0, len(items))
	urls := make([]*model.LabelURL, 0)
	subLabels := make([]*model.LabelSubLabel, 0)
	for _, item := range items {
		if item == nil {
			continue
		}
		item.observedAt = observedAt
		rootIDs = append(rootIDs, item.ID)
		urls = append(urls, item.GetUrls()...)
		subLabels = append(subLabels, item.GetSubLabels()...)
	}
	var urlError, subLabelError error
	urls, urlError = deduplicateLabelURLs(urls)
	subLabels, subLabelError = deduplicateLabelSubLabels(subLabels)
	if deduplicateError := errors.Join(urlError, subLabelError); deduplicateError != nil {
		return result.NewResult(0, deduplicateError)
	}
	rootIDs = deduplicateComparable(rootIDs)
	return executeChunk(order, chunk, func(transactionOrder Order) result.Result {
		existingRoots, err := findExistingRelationRoots(
			transactionOrder,
			rootIDs,
			relationRootTable{labelURLRelation.table, labelURLRelation.parentColumn},
			relationRootTable{labelSubLabelRelation.table, labelSubLabelRelation.parentColumn},
		)
		if err != nil {
			return result.NewResult(0, err)
		}
		written := result.NewResult(0, nil)
		reconcile := []func() result.Result{
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					labelURLRelation,
					len(existingRoots.forTable(labelURLRelation.table)) > 0,
					existingRoots.forTable(labelURLRelation.table),
					urls,
					func(item *model.LabelURL) int32 { return item.LabelID },
					func(item *model.LabelURL) int32 { return item.Hash },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					labelSubLabelRelation,
					len(existingRoots.forTable(labelSubLabelRelation.table)) > 0,
					existingRoots.forTable(labelSubLabelRelation.table),
					subLabels,
					func(item *model.LabelSubLabel) int32 { return item.ParentLabelID },
					func(item *model.LabelSubLabel) int32 { return item.SubLabelID },
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
