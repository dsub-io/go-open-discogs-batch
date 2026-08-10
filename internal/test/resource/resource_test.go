package resource

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadReturnsBytesAndPanicsOnFilesystemErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture")
	require.NoError(t, os.WriteFile(path, []byte("payload"), 0o600))
	require.Equal(t, []byte("payload"), Read(path))
	require.Panics(t, func() { Read(filepath.Join(t.TempDir(), "missing")) })
	require.Panics(t, func() { Read(t.TempDir()) })
}
