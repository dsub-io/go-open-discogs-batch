package batch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/knadh/koanf"
	"github.com/moby/moby/api/types/mount"
	dockerclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/gorm"
)

const (
	restartPostgresImage         = "postgres:18.4-alpine"
	restartPostgresDataDirectory = "/var/lib/postgresql"
	restartPostgresDatabase      = "test_db"
	restartPostgresPort          = "5432/tcp"
	restartPostgresReadyLog      = "database system is ready to accept connections"
	restartPostgresResourceLabel = "io.dsub.test-resource"
	restartPostgresResourceValue = "postgres-forced-restart"
	restartPostgresOwnerLabel    = "io.dsub.test-owner"
	restartPostgresOwnerValue    = "go-open-discogs-batch"
	restartPostgresRunLabel      = "io.dsub.test-run"
)

func TestReleaseResumesAfterPostgresIsKilledAndRestarted(t *testing.T) {
	ctx := context.Background()
	container, connection := startRestartablePostgres(t, ctx)

	db, err := database.GetConnect(testutils.GetDsn(testutils.Postgres, connection))
	require.NoError(t, err)
	require.NoError(t, RunDDL(db))
	cache.ResetIDs()
	t.Cleanup(cache.ResetIDs)
	seedReleaseReferences(t, ctx, db)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	dump := importDump(releaseEntityType, "2026-07-01", "a")
	coordinator := NewImportExecutionCoordinator(sqlDB, "postgres-kill-test")
	preparation, err := coordinator.Prepare(
		ctx,
		[]*model.DiscogsDump{dump},
		1,
		false,
		false,
	)
	require.NoError(t, err)

	committed := make(chan struct{})
	releaseWriter := make(chan struct{})
	pipelineResult := make(chan result.Result, 1)
	pipelineContext, cancelPipeline := context.WithCancel(ctx)
	go func() {
		pipelineResult <- processRelationChunks(
			NewTrackedOrder(
				pipelineContext,
				1,
				1,
				"testdata/release.xml.gz",
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
					close(committed)
					<-releaseWriter
				}
				return written
			},
		)
	}()

	select {
	case <-committed:
	case <-time.After(chunkSynchronizationTimeout):
		t.Fatal("timed out waiting for the committed Release chunk")
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, docker.Close()) })
	_, err = docker.ContainerKill(
		ctx,
		container.GetContainerID(),
		dockerclient.ContainerKillOptions{Signal: "KILL"},
	)
	require.NoError(t, err)
	require.NoError(t, waitForContainerStopped(ctx, container))
	cancelPipeline()
	close(releaseWriter)
	interrupted := <-pipelineResult
	require.Error(t, interrupted.Err())
	releaseContext, cancelRelease := context.WithTimeout(ctx, 2*time.Second)
	coordinator.release(releaseContext)
	cancelRelease()
	require.NoError(t, sqlDB.Close())

	require.NoError(t, container.Start(ctx))
	restartedPort, err := container.MappedPort(ctx, restartPostgresPort)
	require.NoError(t, err)
	connection.Port = restartedPort.Port()
	restarted, err := connectAfterPostgresRestart(
		ctx,
		testutils.GetDsn(testutils.Postgres, connection),
	)
	require.NoError(t, err)
	restartedSQL, err := restarted.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restartedSQL.Close()) })
	require.Equal(
		t,
		[]int64{0},
		completedChunkIndexes(t, restarted, preparation.RunID, releaseEntityType),
	)

	cache.ResetIDs()
	config := koanf.New(".")
	require.NoError(t, config.Set("artists", false))
	require.NoError(t, config.Set("labels", false))
	require.NoError(t, config.Set("masters", false))
	require.NoError(t, config.Set("releases", true))
	require.NoError(t, preloadReferenceIDs(ctx, restartedSQL, config))
	retry := NewImportExecutionCoordinator(restartedSQL, "postgres-kill-test")
	retried, err := retry.Prepare(
		ctx,
		[]*model.DiscogsDump{dump},
		1,
		false,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, preparation.RunID, retried.ResumedFromRunID)

	resumed := processRelationChunksWithFinalizer(
		NewTrackedOrder(
			ctx,
			1,
			1,
			"testdata/release.xml.gz",
			restarted,
			retried.RunID,
			releaseEntityType,
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
	require.NoError(t, resumed.Err())
	require.NoError(t, retry.Complete(ctx, nil))
	assertCompletedRunSummary(t, restarted, retried.RunID, releaseEntityType, 3, 3)
}

func waitForContainerStopped(ctx context.Context, container testcontainers.Container) error {
	deadline, cancel := context.WithTimeout(ctx, chunkSynchronizationTimeout)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := container.State(deadline)
		if err != nil {
			return fmt.Errorf("inspect killed PostgreSQL container: %w", err)
		}
		if !state.Running {
			return nil
		}
		select {
		case <-deadline.Done():
			return errors.New("timed out waiting for killed PostgreSQL container to stop")
		case <-ticker.C:
		}
	}
}

func connectAfterPostgresRestart(ctx context.Context, dsn string) (*gorm.DB, error) {
	deadline, cancel := context.WithTimeout(ctx, chunkSynchronizationTimeout)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		db, err := database.GetConnect(dsn)
		if err == nil {
			return db, nil
		}
		lastErr = err
		select {
		case <-deadline.Done():
			return nil, fmt.Errorf("connect after PostgreSQL restart: %w", lastErr)
		case <-ticker.C:
		}
	}
}

func seedReleaseReferences(t *testing.T, ctx context.Context, db *gorm.DB) {
	t.Helper()
	for _, seed := range []struct {
		path string
		step func(Order) Step
	}{
		{path: "testdata/artist.xml.gz", step: newBatch().UpdateArtist},
		{path: "testdata/label.xml.gz", step: newBatch().UpdateLabel},
		{path: "testdata/master.xml.gz", step: newBatch().UpdateMaster},
	} {
		seedResult := seed.step(NewOrder(ctx, 1, 1, seed.path, db))()
		require.NoError(t, seedResult.Err())
	}
}

func startRestartablePostgres(
	t *testing.T,
	ctx context.Context,
) (testcontainers.Container, testutils.Database) {
	t.Helper()
	testRunID, err := testutils.CurrentTestRunID()
	require.NoError(t, err)
	resourceID := fmt.Sprintf("postgres-restart-%d-%d", os.Getpid(), time.Now().UTC().UnixNano())
	volumeName := "open-discogs-" + resourceID
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	require.NoError(t, err)
	_, err = docker.VolumeCreate(ctx, dockerclient.VolumeCreateOptions{
		Name: volumeName,
		Labels: map[string]string{
			restartPostgresOwnerLabel:    restartPostgresOwnerValue,
			restartPostgresRunLabel:      testRunID,
			restartPostgresResourceLabel: restartPostgresResourceValue,
		},
	})
	require.NoError(t, err)

	var container testcontainers.Container
	cleanup := func() {
		if container != nil {
			if terminateErr := container.Terminate(ctx); terminateErr != nil {
				t.Errorf("terminate restartable PostgreSQL container: %v", terminateErr)
			}
			if _, inspectErr := docker.ContainerInspect(
				ctx,
				container.GetContainerID(),
				dockerclient.ContainerInspectOptions{},
			); inspectErr == nil {
				t.Errorf("restartable PostgreSQL container was not removed")
			}
		}
		if _, removeErr := docker.VolumeRemove(
			ctx,
			volumeName,
			dockerclient.VolumeRemoveOptions{Force: true},
		); removeErr != nil {
			t.Errorf("remove restartable PostgreSQL volume: %v", removeErr)
		}
		if _, inspectErr := docker.VolumeInspect(
			ctx,
			volumeName,
			dockerclient.VolumeInspectOptions{},
		); inspectErr == nil {
			t.Errorf("restartable PostgreSQL volume was not removed")
		}
		if closeErr := docker.Close(); closeErr != nil {
			t.Errorf("close restartable PostgreSQL Docker client: %v", closeErr)
		}
	}
	t.Cleanup(cleanup)

	container, err = testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image: restartPostgresImage,
				Env: map[string]string{
					"POSTGRES_DB":       restartPostgresDatabase,
					"POSTGRES_PASSWORD": restartPostgresDatabase,
					"POSTGRES_USER":     restartPostgresDatabase,
				},
				ExposedPorts: []string{restartPostgresPort},
				Labels: map[string]string{
					restartPostgresOwnerLabel:    restartPostgresOwnerValue,
					restartPostgresRunLabel:      testRunID,
					restartPostgresResourceLabel: restartPostgresResourceValue,
				},
				Mounts: testcontainers.Mounts(
					testcontainers.VolumeMount(volumeName, restartPostgresDataDirectory),
				),
				WaitingFor: wait.ForAll(
					wait.ForLog(restartPostgresReadyLog),
					wait.ForListeningPort(restartPostgresPort),
				).WithDeadline(120 * time.Second),
			},
			Started: true,
		},
	)
	if err != nil {
		t.Fatalf("start restartable PostgreSQL container: %v", err)
	}
	inspection, err := container.Inspect(ctx)
	require.NoError(t, err)
	require.Len(t, inspection.Mounts, 1)
	require.Equal(t, mount.TypeVolume, inspection.Mounts[0].Type)
	require.Equal(t, volumeName, inspection.Mounts[0].Name)
	require.Equal(t, restartPostgresDataDirectory, inspection.Mounts[0].Destination)
	require.True(t, inspection.Mounts[0].RW)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, restartPostgresPort)
	require.NoError(t, err)
	return container, testutils.Database{
		Username: restartPostgresDatabase,
		Password: restartPostgresDatabase,
		Hostname: host,
		DBName:   restartPostgresDatabase,
		Type:     testutils.Postgres,
		Port:     port.Port(),
	}
}
