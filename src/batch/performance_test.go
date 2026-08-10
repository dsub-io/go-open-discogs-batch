package batch

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const (
	benchmarkCheckpointView = "discogs_import_checkpoint"
	benchmarkChunkSize      = 5
	benchmarkMaxWorkers     = 2
)

var benchmarkFixtures = []struct {
	path string
	step func(Order) Step
}{
	{path: "testdata/artist.xml.gz", step: newBatch().UpdateArtist},
	{path: "testdata/label.xml.gz", step: newBatch().UpdateLabel},
	{path: "testdata/master.xml.gz", step: newBatch().UpdateMaster},
	{path: "testdata/release.xml.gz", step: newBatch().UpdateRelease},
}

func BenchmarkBatchImport(b *testing.B) {
	pg := testutils.GetDatabase(b, testutils.Postgres)
	db, err := database.GetConnect(testutils.GetDsn(testutils.Postgres, pg))
	require.NoError(b, err)
	require.NoError(b, RunDDL(db))
	fixtureBytes := benchmarkFixtureBytes(b)

	b.Run("initial", func(b *testing.B) {
		b.SetBytes(fixtureBytes)
		for range b.N {
			b.StopTimer()
			resetBenchmarkDatabase(b, db)
			b.StartTimer()
			runBenchmarkImport(b, db)
		}
	})

	b.Run("repeat", func(b *testing.B) {
		b.StopTimer()
		resetBenchmarkDatabase(b, db)
		runBenchmarkImport(b, db)
		b.StartTimer()
		b.SetBytes(fixtureBytes)
		for range b.N {
			runBenchmarkImport(b, db)
		}
	})
}

func benchmarkFixtureBytes(b *testing.B) int64 {
	b.Helper()
	var total int64
	for _, fixture := range benchmarkFixtures {
		info, err := os.Stat(fixture.path)
		require.NoError(b, err)
		total += info.Size()
	}
	return total
}

func resetBenchmarkDatabase(b *testing.B, db *gorm.DB) {
	b.Helper()
	tables := make([]string, 0, len(model.TableNames))
	for _, table := range model.TableNames {
		if table == benchmarkCheckpointView {
			continue
		}
		tables = append(tables, fmt.Sprintf(`public."%s"`, table))
	}
	statement := fmt.Sprintf(
		"truncate table %s restart identity cascade",
		strings.Join(tables, ", "),
	)
	require.NoError(b, db.Exec(statement).Error)
	cache.ResetIDs()
}

func runBenchmarkImport(b *testing.B, db *gorm.DB) {
	b.Helper()
	ctx := context.Background()
	for _, fixture := range benchmarkFixtures {
		result := fixture.step(NewOrder(
			ctx,
			benchmarkChunkSize,
			benchmarkMaxWorkers,
			fixture.path,
			db,
		))()
		require.NoError(b, result.Err())
	}
}
