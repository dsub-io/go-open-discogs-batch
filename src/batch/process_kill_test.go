package batch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/knadh/koanf"
	"github.com/stretchr/testify/require"
)

const (
	releaseProcessKillHelper   = "OPEN_DISCOGS_RELEASE_PROCESS_KILL_HELPER"
	releaseProcessKillDSN      = "OPEN_DISCOGS_RELEASE_PROCESS_KILL_DSN"
	releaseProcessKillPath     = "OPEN_DISCOGS_RELEASE_PROCESS_KILL_PATH"
	releaseProcessKillSignalFD = 3
)

func TestReleaseProcessKillResume(t *testing.T) {
	postgres := testutils.GetDatabase(t, testutils.Postgres)
	dsn := testutils.GetDsn(testutils.Postgres, postgres)
	runReleaseProcessKillResume(t, dsn)
}

func TestReleaseProcessKillHelper(t *testing.T) {
	if os.Getenv(releaseProcessKillHelper) != "1" {
		return
	}

	ctx := context.Background()
	dsn := os.Getenv(releaseProcessKillDSN)
	releasePath := os.Getenv(releaseProcessKillPath)
	require.NotEmpty(t, dsn)
	require.NotEmpty(t, releasePath)
	db, err := database.GetConnect(dsn)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	config := koanf.New(".")
	require.NoError(t, config.Set("artists", false))
	require.NoError(t, config.Set("labels", false))
	require.NoError(t, config.Set("masters", false))
	require.NoError(t, config.Set("releases", true))
	require.NoError(t, preloadReferenceIDs(ctx, sqlDB, config))

	dump := importDump(releaseEntityType, "2026-07-01", "9")
	coordinator := NewImportExecutionCoordinator(sqlDB, "process-kill-test")
	preparation, err := coordinator.Prepare(
		ctx,
		[]*model.DiscogsDump{dump},
		1,
		false,
		false,
	)
	require.NoError(t, err)
	signal := os.NewFile(releaseProcessKillSignalFD, "release-process-kill-signal")
	require.NotNil(t, signal)
	t.Cleanup(func() { require.NoError(t, signal.Close()) })

	result := processRelationChunks(
		NewTrackedOrder(
			ctx,
			1,
			1,
			releasePath,
			db,
			preparation.RunID,
			releaseEntityType,
			false,
		),
		"release relations",
		"release",
		"source-read release relations",
		func(order Order, chunk ChunkMetadata, items []*XmlReleaseRelation) result.Result {
			written := writeReleaseRelationChunk(order, chunk, items)
			if chunk.Index == 0 && !written.IsErr() {
				_, writeErr := signal.Write([]byte{1})
				if writeErr != nil {
					return result.NewResult(0, writeErr)
				}
				select {}
			}
			return written
		},
	)
	t.Fatalf("release process-kill helper returned unexpectedly: %v", result.Err())
}

func runReleaseProcessKillResume(t *testing.T, dsn string) {
	const (
		chunkSize  = 1
		maxWorkers = 1
		entityType = releaseEntityType
	)
	ctx := context.Background()
	db := resetProgressDatabase(t, dsn)
	sqlDB, err := db.DB()
	require.NoError(t, err)

	cache.ResetIDs()
	for _, seed := range []struct {
		path string
		step func(Order) Step
	}{
		{path: "testdata/artist.xml.gz", step: newBatch().UpdateArtist},
		{path: "testdata/label.xml.gz", step: newBatch().UpdateLabel},
		{path: "testdata/master.xml.gz", step: newBatch().UpdateMaster},
	} {
		seedResult := seed.step(NewOrder(ctx, chunkSize, maxWorkers, seed.path, db))()
		require.NoError(t, seedResult.Err())
	}

	releasePath, err := filepath.Abs("testdata/release.xml.gz")
	require.NoError(t, err)
	signalReader, signalWriter, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, signalReader.Close()) })
	command := exec.Command(os.Args[0], "-test.run=^TestReleaseProcessKillHelper$", "-test.v")
	command.Env = append(
		os.Environ(),
		fmt.Sprintf("%s=1", releaseProcessKillHelper),
		fmt.Sprintf("%s=%s", releaseProcessKillDSN, dsn),
		fmt.Sprintf("%s=%s", releaseProcessKillPath, releasePath),
	)
	command.ExtraFiles = []*os.File{signalWriter}
	command.Stdout = os.Stderr
	command.Stderr = os.Stderr
	require.NoError(t, command.Start())
	require.NoError(t, signalWriter.Close())
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})

	signaled := make(chan error, 1)
	go func() {
		var signal [1]byte
		_, readErr := io.ReadFull(signalReader, signal[:])
		signaled <- readErr
	}()
	select {
	case signalErr := <-signaled:
		require.NoError(t, signalErr)
	case <-time.After(chunkSynchronizationTimeout):
		t.Fatal("timed out waiting for the committed Release chunk")
	}
	require.NoError(t, command.Process.Kill())
	waitErr := command.Wait()
	var exitError *exec.ExitError
	require.ErrorAs(t, waitErr, &exitError)

	var abandonedRunID int64
	require.NoError(t, db.Raw(
		`select id
		   from discogs_import_run
		  where status = 'running'
		  order by id desc
		  limit 1`,
	).Scan(&abandonedRunID).Error)
	require.Positive(t, abandonedRunID)
	require.Equal(
		t,
		[]int64{0},
		completedChunkIndexes(t, db, abandonedRunID, entityType),
	)

	cache.ResetIDs()
	config := koanf.New(".")
	require.NoError(t, config.Set("artists", false))
	require.NoError(t, config.Set("labels", false))
	require.NoError(t, config.Set("masters", false))
	require.NoError(t, config.Set("releases", true))
	require.NoError(t, preloadReferenceIDs(ctx, sqlDB, config))
	dump := importDump(entityType, "2026-07-01", "9")
	retry := NewImportExecutionCoordinator(sqlDB, "process-kill-test")
	retryPreparation, err := retry.Prepare(
		ctx,
		[]*model.DiscogsDump{dump},
		chunkSize,
		false,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, abandonedRunID, retryPreparation.ResumedFromRunID)
	require.Empty(t, completedChunkIndexes(t, db, abandonedRunID, entityType))

	retried := processRelationChunksWithFinalizer(
		NewTrackedOrder(
			ctx,
			chunkSize,
			maxWorkers,
			releasePath,
			db,
			retryPreparation.RunID,
			entityType,
			true,
		),
		"release relations",
		"release",
		"source-read release relations",
		func(order Order, chunk ChunkMetadata, items []*XmlReleaseRelation) result.Result {
			if chunk.Index == 0 {
				return result.NewResult(0, errors.New("committed Release chunk was rewritten"))
			}
			return writeReleaseRelationChunk(order, chunk, items)
		},
		finalizeReleaseImport,
	)
	require.NoError(t, retried.Err())
	require.NoError(t, retry.Complete(ctx, nil))
	assertCompletedRunSummary(t, db, retryPreparation.RunID, entityType, 3, 3)
	require.Empty(t, completedChunkIndexes(t, db, retryPreparation.RunID, entityType))
}
