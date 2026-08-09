package batch

import (
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
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
		"updating master relations...",
		writeMasterRelationChunk,
	)
}

func writeMasterRelationChunk(order Order, items []*XmlMasterRelation) result.Result {
	s := make([]*model.Style, 0)
	g := make([]*model.Genre, 0)
	for _, item := range items {
		if item == nil {
			continue
		}
		g = append(g, item.GetGenres()...)
		s = append(s, item.GetStyles()...)
	}
	s = filterStyles(s)
	g = filterGenres(g)
	if referenceResult := writeReferenceEntities(order, g, s); referenceResult.IsErr() {
		return referenceResult
	}

	var (
		m  = make([]*model.Master, 0)
		mv = make([]*model.MasterVideo, 0)
		ms = make([]*model.MasterStyle, 0)
		mg = make([]*model.MasterGenre, 0)
		ma = make([]*model.MasterArtist, 0)
	)
	for _, item := range items {
		if item == nil {
			continue
		}
		master := item.GetMaster()
		m = append(m, master)
		cache.MasterIDCache.Store(master.ID, struct{}{})
		ms = append(ms, item.GetMasterStyles()...)
		mg = append(mg, item.GetMasterGenres()...)
		mv = append(mv, item.GetMasterVideos()...)
		ma = append(ma, item.GetMasterArtists()...)
	}
	return writeChunk(order, m, mv, ms, mg, ma)
}
