package batch

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	fileutil "github.com/dsub-io/go-open-discogs-batch/src/file"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const (
	benchmarkCheckpointView = "discogs_import_checkpoint"
	benchmarkChunkSize      = 5
	benchmarkMaxWorkers     = 2
	benchmarkPreflightBytes = 64 << 20
)

var benchmarkFixtures = []struct {
	entityType string
	path       string
	step       func(Order) Step
}{
	{entityType: "artist", path: "testdata/artist.xml.gz", step: newBatch().UpdateArtist},
	{entityType: "label", path: "testdata/label.xml.gz", step: newBatch().UpdateLabel},
	{entityType: "master", path: "testdata/master.xml.gz", step: newBatch().UpdateMaster},
	{entityType: "release", path: "testdata/release.xml.gz", step: newBatch().UpdateRelease},
}

func BenchmarkCompletedManifestPreflight(b *testing.B) {
	restoreOutput := silenceBenchmarkOutput(b)
	defer restoreOutput()
	ctx := context.Background()
	pg := testutils.GetDatabase(b, testutils.Postgres)
	db, err := database.GetConnect(testutils.GetDsn(testutils.Postgres, pg))
	require.NoError(b, err)
	require.NoError(b, RunDDL(db))
	sqlDB, err := db.DB()
	require.NoError(b, err)
	dumps := []*model.DiscogsDump{
		importDump("artist", "2026-07-01", "c"),
	}
	completed := NewImportExecutionCoordinator(sqlDB, "preflight-benchmark")
	preparation, err := completed.Prepare(
		ctx,
		dumps,
		benchmarkChunkSize,
		false,
		false,
	)
	require.NoError(b, err)
	order := NewTrackedOrder(
		ctx,
		benchmarkChunkSize,
		benchmarkMaxWorkers,
		"unused",
		db,
		preparation.RunID,
		"artist",
		false,
	)
	require.NoError(b, completeEntityProgress(order, 0, 0))
	require.NoError(b, completed.Complete(ctx, nil))

	resourcePath, checksum := benchmarkPreflightResource(b)
	handler := fileutil.NewHandler()
	b.SetBytes(benchmarkPreflightBytes)

	b.Run("checksum_before_admission", func(b *testing.B) {
		b.SetBytes(benchmarkPreflightBytes)
		for range b.N {
			require.NoError(b, handler.Checksum(resourcePath, checksum))
			prepareCompletedManifest(b, ctx, sqlDB, dumps)
		}
	})

	b.Run("admission_before_checksum", func(b *testing.B) {
		b.SetBytes(benchmarkPreflightBytes)
		for range b.N {
			prepareCompletedManifest(b, ctx, sqlDB, dumps)
		}
	})
}

func benchmarkPreflightResource(b *testing.B) (string, string) {
	b.Helper()
	resource, err := os.CreateTemp(b.TempDir(), "completed-manifest-*.xml.gz")
	require.NoError(b, err)
	require.NoError(b, resource.Truncate(benchmarkPreflightBytes))
	_, err = resource.Seek(0, io.SeekStart)
	require.NoError(b, err)
	hash := sha256.New()
	_, err = io.Copy(hash, resource)
	require.NoError(b, err)
	require.NoError(b, resource.Close())
	return resource.Name(), hex.EncodeToString(hash.Sum(nil))
}

func prepareCompletedManifest(
	b *testing.B,
	ctx context.Context,
	db *sql.DB,
	dumps []*model.DiscogsDump,
) {
	b.Helper()
	coordinator := NewImportExecutionCoordinator(db, "preflight-benchmark")
	preparation, err := coordinator.Prepare(
		ctx,
		dumps,
		benchmarkChunkSize,
		false,
		false,
	)
	require.NoError(b, err)
	require.True(b, preparation.Skipped)
}

func BenchmarkBatchImport(b *testing.B) {
	restoreOutput := silenceBenchmarkOutput(b)
	defer restoreOutput()
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

func BenchmarkDurableBatchImport(b *testing.B) {
	restoreOutput := silenceBenchmarkOutput(b)
	defer restoreOutput()
	pg := testutils.GetDatabase(b, testutils.Postgres)
	db, err := database.GetConnect(testutils.GetDsn(testutils.Postgres, pg))
	require.NoError(b, err)
	require.NoError(b, RunDDL(db))
	fixtureBytes := benchmarkFixtureBytes(b)
	dumps := benchmarkImportDumps()

	b.Run("initial", func(b *testing.B) {
		b.SetBytes(fixtureBytes)
		for range b.N {
			b.StopTimer()
			resetBenchmarkDatabase(b, db)
			b.StartTimer()
			runDurableBenchmarkImport(b, db, dumps, false)
		}
	})

	b.Run("forced_repeat", func(b *testing.B) {
		b.StopTimer()
		resetBenchmarkDatabase(b, db)
		runDurableBenchmarkImport(b, db, dumps, false)
		b.StartTimer()
		b.SetBytes(fixtureBytes)
		for range b.N {
			runDurableBenchmarkImport(b, db, dumps, true)
		}
	})
}

func benchmarkImportDumps() []*model.DiscogsDump {
	dumps := make([]*model.DiscogsDump, 0, len(benchmarkFixtures))
	for index, fixture := range benchmarkFixtures {
		dumps = append(dumps, importDump(
			fixture.entityType,
			"2026-07-01",
			fmt.Sprintf("%x", index+1),
		))
	}
	return dumps
}

func runDurableBenchmarkImport(
	b *testing.B,
	db *gorm.DB,
	dumps []*model.DiscogsDump,
	force bool,
) {
	b.Helper()
	ctx := context.Background()
	sqlDB, err := db.DB()
	require.NoError(b, err)
	coordinator := NewImportExecutionCoordinator(sqlDB, "durable-benchmark")
	preparation, err := coordinator.Prepare(
		ctx,
		dumps,
		benchmarkChunkSize,
		force,
		false,
	)
	require.NoError(b, err)
	require.False(b, preparation.Skipped)
	for _, fixture := range benchmarkFixtures {
		result := fixture.step(NewTrackedOrder(
			ctx,
			benchmarkChunkSize,
			benchmarkMaxWorkers,
			fixture.path,
			db,
			preparation.RunID,
			fixture.entityType,
			false,
		))()
		require.NoError(b, result.Err())
	}
	require.NoError(b, coordinator.Complete(ctx, nil))
}

func silenceBenchmarkOutput(b *testing.B) func() {
	b.Helper()
	output, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	require.NoError(b, err)
	standardOutput := os.Stdout
	standardError := os.Stderr
	os.Stdout = output
	os.Stderr = output
	return func() {
		os.Stdout = standardOutput
		os.Stderr = standardError
		require.NoError(b, output.Close())
	}
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
