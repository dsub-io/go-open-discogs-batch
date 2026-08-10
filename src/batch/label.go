package batch

import (
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/go-open-discogs-batch/src/unique"
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
	return InsertSimple[XmlLabel, model.Label](order, "labels", "label")
}

func insertLabelRelations(order Order) result.Result {
	deleteStale, err := relationTablesContainRows(
		order,
		labelSubLabelRelation.table,
		labelURLRelation.table,
	)
	if err != nil {
		return result.NewResult(0, err)
	}
	return processRelationChunks(
		order,
		"label relations",
		"label",
		"updating label relations...",
		func(order Order, chunk ChunkMetadata, items []*XmlLabelRelation) result.Result {
			return writeLabelRelationChunk(order, chunk, items, deleteStale)
		},
	)
}

func writeLabelRelationChunk(
	order Order,
	chunk ChunkMetadata,
	items []*XmlLabelRelation,
	deleteStale bool,
) result.Result {
	return executeChunk(order, chunk, func(transactionOrder Order) result.Result {
		rootIDs := make([]int32, 0, len(items))
		labels := make([]*model.Label, 0, len(items))
		urls := make([]*model.LabelURL, 0)
		subLabels := make([]*model.LabelSubLabel, 0)
		for _, item := range items {
			if item == nil {
				continue
			}
			rootIDs = append(rootIDs, item.ID)
			labels = append(labels, item.GetLabel())
			urls = append(urls, item.GetUrls()...)
			subLabels = append(subLabels, item.GetSubLabels()...)
		}
		rootIDs = unique.Slice(rootIDs)
		written := writeChunk(transactionOrder, labels)
		if written.IsErr() {
			return written
		}
		reconcile := []func() result.Result{
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					labelURLRelation,
					deleteStale,
					rootIDs,
					urls,
					func(item *model.LabelURL) int32 { return item.LabelID },
					func(item *model.LabelURL) int32 { return item.Hash },
				)
			},
			func() result.Result {
				return reconcileIntegerRelation(
					transactionOrder,
					labelSubLabelRelation,
					deleteStale,
					rootIDs,
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
