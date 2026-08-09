package batch

import (
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
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
	return processRelationChunks(
		order,
		"label relations",
		"label",
		"updating label relations...",
		writeLabelRelationChunk,
	)
}

func writeLabelRelationChunk(order Order, items []*XmlLabelRelation) result.Result {
	u := make([]*model.LabelURL, 0)
	s := make([]*model.LabelSubLabel, 0)
	for _, item := range items {
		u = append(u, item.GetUrls()...)
		s = append(s, item.GetSubLabels()...)
	}
	return writeChunk(order, u, s)
}
