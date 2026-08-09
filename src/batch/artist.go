package batch

import (
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
)

func GetArtistStep(order Order) Step {
	return func() result.Result {
		updated := 0
		res := insertArtists(order)
		updated += res.Count()
		if res.IsErr() {
			return result.NewResult(updated, res.Err())
		}
		res = insertArtistRelations(order)
		updated += res.Count()
		if res.IsErr() {
			return result.NewResult(updated, res.Err())
		}
		return result.NewResult(updated, nil)
	}
}

func insertArtists(order Order) result.Result {
	return InsertSimple[XmlArtist, model.Artist](order, "artists", "artist")
}

func insertArtistRelations(order Order) result.Result {
	return processRelationChunks(
		order,
		"artist relations",
		"artist",
		"updating artist relations...",
		writeArtistRelationChunk,
	)
}

func writeArtistRelationChunk(order Order, items []*XmlArtistRelation) result.Result {
	n := make([]*model.ArtistNameVariation, 0)
	a := make([]*model.ArtistAlias, 0)
	g := make([]*model.ArtistGroup, 0)
	m := make([]*model.ArtistMember, 0)
	u := make([]*model.ArtistURL, 0)
	for _, item := range items {
		a = append(a, item.GetAliases()...)
		g = append(g, item.GetGroups()...)
		m = append(m, item.GetMembers()...)
		n = append(n, item.GetNameVars()...)
		u = append(u, item.GetUrls()...)
	}
	return writeChunk(order, a, g, m, n, u)
}
