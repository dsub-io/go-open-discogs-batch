package batch

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/src/data"
	"github.com/stretchr/testify/require"
)

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
