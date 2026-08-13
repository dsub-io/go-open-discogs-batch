//go:build fulldump

package batch

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/reader"
)

const releaseAuditPathEnvironment = "OPEN_DISCOGS_RELEASE_DUMP"

// TestReleaseFullDumpCanonicalAudit streams one monthly dump without retaining
// cross-release state. Enable it explicitly with the fulldump build tag.
func TestReleaseFullDumpCanonicalAudit(t *testing.T) {
	path := os.Getenv(releaseAuditPathEnvironment)
	if path == "" {
		t.Fatalf("%s is required", releaseAuditPathEnvironment)
	}
	source, err := newReadCloser(path, "audit release canonical identities")
	if err != nil {
		t.Fatal(err)
	}

	startedAt := time.Now()
	observedAt := startedAt.UTC()
	var roots int64
	var exactDuplicates int64
	var previousRootID int32
	for result := range reader.NewReader[XmlReleaseRelation](
		context.Background(),
		source,
		"release",
	).Observe() {
		if result.E != nil {
			t.Fatalf("parse release after %d roots: %v", roots, result.E)
		}
		release, ok := result.V.(*XmlReleaseRelation)
		if !ok || release == nil {
			t.Fatalf("unexpected release value after %d roots", roots)
		}
		if roots > 0 && release.ID <= previousRootID {
			t.Fatalf(
				"release root order is not strictly increasing: previous=%d current=%d",
				previousRootID,
				release.ID,
			)
		}
		previousRootID = release.ID
		release.setObservedAt(observedAt)

		duplicateCount, auditErr := auditReleaseCanonicalRelations(release)
		if auditErr != nil {
			t.Fatalf("release %d canonical identity: %v", release.ID, auditErr)
		}
		exactDuplicates += int64(duplicateCount)
		roots++
	}

	duration := time.Since(startedAt)
	if roots == 0 {
		t.Fatal("release dump contained no roots")
	}
	t.Logf(
		"audited roots=%d exact_duplicates=%d duration=%s roots_per_second=%.0f",
		roots,
		exactDuplicates,
		duration.Round(time.Millisecond),
		float64(roots)/duration.Seconds(),
	)
}

func auditReleaseCanonicalRelations(release *XmlReleaseRelation) (int, error) {
	checks := []func() (int, error){
		func() (int, error) { return auditCanonicalRows(release.GetReleaseArtists(), deduplicateReleaseArtists) },
		func() (int, error) {
			return auditCanonicalRows(release.GetCreditedArtists(), deduplicateReleaseCreditedArtists)
		},
		func() (int, error) { return auditCanonicalRows(release.GetWorks(), deduplicateReleaseWorks) },
		func() (int, error) { return auditCanonicalRows(release.GetFormats(), deduplicateReleaseFormats) },
		func() (int, error) { return auditCanonicalRows(release.GetReleaseStyles(), deduplicateReleaseStyles) },
		func() (int, error) { return auditCanonicalRows(release.GetReleaseGenres(), deduplicateReleaseGenres) },
		func() (int, error) { return auditCanonicalRows(release.GetLabels(), deduplicateLabelReleaseItems) },
		func() (int, error) {
			return auditCanonicalRows(release.GetIdentifiers(), deduplicateReleaseIdentifiers)
		},
		func() (int, error) { return auditCanonicalRows(release.GetTracks(), deduplicateReleaseTracks) },
		func() (int, error) { return auditCanonicalRows(release.GetVideos(), deduplicateReleaseVideos) },
	}
	totalDuplicates := 0
	for index, check := range checks {
		duplicates, err := check()
		if err != nil {
			return 0, fmt.Errorf("relation check %d: %w", index, err)
		}
		totalDuplicates += duplicates
	}
	return totalDuplicates, nil
}

func auditCanonicalRows[T any](
	rows []T,
	deduplicate func([]T) ([]T, error),
) (int, error) {
	canonical, err := deduplicate(rows)
	if err != nil {
		return 0, err
	}
	return len(rows) - len(canonical), nil
}
