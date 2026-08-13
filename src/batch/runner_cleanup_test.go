package batch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/data"
	"github.com/stretchr/testify/require"
)

type completionStub struct {
	complete func(context.Context, error) error
}

func (s completionStub) Complete(ctx context.Context, runErr error) error {
	return s.complete(ctx, runErr)
}

func TestCleanupImportFilesDeletesOnlySelectedResources(t *testing.T) {
	directory := t.TempDir()
	artist := filepath.Join(directory, "artist.xml.gz")
	release := filepath.Join(directory, "release.xml.gz")
	unrelated := filepath.Join(directory, "keep.txt")
	for _, path := range []string{artist, release, unrelated} {
		require.NoError(t, os.WriteFile(path, []byte("fixture"), 0600))
	}
	plan := &data.ImportPlan{
		Resources: map[string]string{"artists": artist, "releases": release},
	}

	require.NoError(t, cleanupImportFiles(plan))
	require.NoFileExists(t, artist)
	require.NoFileExists(t, release)
	require.FileExists(t, unrelated)
}

func TestFinalizeImportCommitsBeforeCleanup(t *testing.T) {
	directory := t.TempDir()
	resource := filepath.Join(directory, "release.xml.gz")
	require.NoError(t, os.WriteFile(resource, []byte("fixture"), 0600))
	plan := &data.ImportPlan{Resources: map[string]string{"releases": resource}}
	completed := false
	completer := completionStub{complete: func(_ context.Context, runErr error) error {
		require.NoError(t, runErr)
		require.FileExists(t, resource)
		completed = true
		return nil
	}}

	err := finalizeImport(context.Background(), completer, plan, true, nil)

	require.NoError(t, err)
	require.True(t, completed)
	require.NoFileExists(t, resource)
}

func TestFinalizeImportRetainsResourcesOnFailure(t *testing.T) {
	directory := t.TempDir()
	resource := filepath.Join(directory, "release.xml.gz")
	require.NoError(t, os.WriteFile(resource, []byte("fixture"), 0600))
	plan := &data.ImportPlan{Resources: map[string]string{"releases": resource}}
	fixtureErr := errors.New("fixture import failure")
	completer := completionStub{complete: func(_ context.Context, runErr error) error {
		require.ErrorIs(t, runErr, fixtureErr)
		return nil
	}}

	err := finalizeImport(context.Background(), completer, plan, true, fixtureErr)

	require.ErrorIs(t, err, fixtureErr)
	require.FileExists(t, resource)
}

func TestFinalizeImportRetainsResourcesWhenCompletionFails(t *testing.T) {
	directory := t.TempDir()
	resource := filepath.Join(directory, "release.xml.gz")
	require.NoError(t, os.WriteFile(resource, []byte("fixture"), 0600))
	plan := &data.ImportPlan{Resources: map[string]string{"releases": resource}}
	completionErr := errors.New("fixture completion failure")
	completer := completionStub{complete: func(_ context.Context, runErr error) error {
		require.NoError(t, runErr)
		return completionErr
	}}

	err := finalizeImport(context.Background(), completer, plan, true, nil)

	require.ErrorIs(t, err, completionErr)
	require.FileExists(t, resource)
}

func TestFinalizeImportUsesCompletionContextAfterCancellation(t *testing.T) {
	directory := t.TempDir()
	resource := filepath.Join(directory, "release.xml.gz")
	require.NoError(t, os.WriteFile(resource, []byte("fixture"), 0600))
	plan := &data.ImportPlan{Resources: map[string]string{"releases": resource}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	completer := completionStub{complete: func(completionCtx context.Context, runErr error) error {
		require.NoError(t, completionCtx.Err())
		require.ErrorIs(t, runErr, context.Canceled)
		return nil
	}}

	err := finalizeImport(ctx, completer, plan, true, context.Canceled)

	require.ErrorIs(t, err, context.Canceled)
	require.FileExists(t, resource)
}

func TestImportCompletionContextDoesNotDeadlineSuccessfulFinalization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	completionCtx, completionCancel := importCompletionContext(ctx, nil)
	defer completionCancel()
	_, hasDeadline := completionCtx.Deadline()

	require.NoError(t, completionCtx.Err())
	require.False(t, hasDeadline)
}

func TestImportCompletionContextBoundsFailureRecording(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	completionCtx, completionCancel := importCompletionContext(
		ctx,
		errors.New("fixture import failure"),
	)
	defer completionCancel()
	deadline, hasDeadline := completionCtx.Deadline()

	require.NoError(t, completionCtx.Err())
	require.True(t, hasDeadline)
	require.WithinDuration(
		t,
		time.Now().Add(failedImportCompletionTimeout),
		deadline,
		time.Second,
	)
}
