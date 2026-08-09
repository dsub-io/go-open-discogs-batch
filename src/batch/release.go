package batch

import (
	"sort"
	"strings"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/go-open-discogs-batch/src/unique"
	"github.com/dsub-io/open-discogs-model/model"
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
		"updating releases...",
		writeReleaseRelationChunk,
	)
}

func writeReleaseRelationChunk(order Order, items []*XmlReleaseRelation) result.Result {
	var (
		g   = make([]*model.Genre, 0)
		s   = make([]*model.Style, 0)
		rel = make([]*model.ReleaseItem, 0)
		ra  = make([]*model.ReleaseItemArtist, 0)
		rca = make([]*model.ReleaseItemCreditedArtist, 0)
		rw  = make([]*model.ReleaseItemWork, 0)
		rf  = make([]*model.ReleaseItemFormat, 0)
		rs  = make([]*model.ReleaseItemStyle, 0)
		rg  = make([]*model.ReleaseItemGenre, 0)
		ri  = make([]*model.ReleaseItemIdentifier, 0)
		rt  = make([]*model.ReleaseItemTrack, 0)
		rv  = make([]*model.ReleaseItemVideo, 0)
		rl  = make([]*model.LabelReleaseItem, 0)
	)
	for _, item := range items {
		if item == nil {
			continue
		}
		g = append(g, item.GetGenres()...)
		s = append(s, item.GetStyles()...)
	}
	if referenceResult := writeReferenceEntities(order, filterGenres(g), filterStyles(s)); referenceResult.IsErr() {
		return referenceResult
	}
	for _, item := range items {
		if item == nil {
			continue
		}
		rel = append(rel, item.GetRelease())
		ra = append(ra, item.GetReleaseArtists()...)
		rg = append(rg, item.GetReleaseGenres()...)
		rs = append(rs, item.GetReleaseStyles()...)
		rw = append(rw, item.GetWorks()...)
		rl = append(rl, item.GetLabels()...)
		rf = append(rf, item.GetFormats()...)
		ri = append(ri, item.GetIdentifiers()...)
		rt = append(rt, item.GetTracks()...)
		rv = append(rv, item.GetVideos()...)
		rca = append(rca, item.GetCreditedArtists()...)
	}
	written := writeChunk(order, rel, ra, rw, rs, rg, rl, rf, ri, rt, rv, rca)
	if !written.IsErr() {
		written = written.Sum(updateMasterMainReleases(order, items))
	}
	return written
}

func updateMasterMainReleases(order Order, releases []*XmlReleaseRelation) result.Result {
	updates := make(map[int32]int32)
	for _, release := range releases {
		if release == nil || !release.MasterInfo.IsMaster || release.MasterInfo.MasterID == nil {
			continue
		}
		updates[*release.MasterInfo.MasterID] = release.ID
	}
	if len(updates) == 0 {
		return result.NewResult(0, nil)
	}

	masterIDs := make([]int32, 0, len(updates))
	for masterID := range updates {
		masterIDs = append(masterIDs, masterID)
	}
	sort.Slice(masterIDs, func(left, right int) bool { return masterIDs[left] < masterIDs[right] })

	updated := 0
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
	return `UPDATE public.master AS target
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
