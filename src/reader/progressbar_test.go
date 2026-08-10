package reader

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"path/filepath"
	"testing"
)

func TestGetFilename(t *testing.T) {
	filename := GetFilename(path.Join("test", "this", "function", "as", "this.txt"))
	assert.Equal(t, "this.txt", filename)
}

func TestNewProgressBarGzipReadCloser(t *testing.T) {
	f, err := os.Open("testdata/data.gz")
	require.NoError(t, err)
	text := "test-progress-bar"
	r, err := NewProgressBarGzipReadCloser(f, text)
	require.NoError(t, err)

	payload := make([]byte, 64)
	n, err := r.Read(payload)
	require.NoError(t, err)
	require.Equal(t, n, 64)

	require.NoError(t, r.Close())
	require.Error(t, f.Close())
}

func TestNewProgressBarGzipReadCloserRejectsInvalidGzip(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "invalid.gz")
	require.NoError(t, os.WriteFile(filePath, []byte("not gzip"), 0o600))
	f, err := os.Open(filePath)
	require.NoError(t, err)

	reader, err := NewProgressBarGzipReadCloser(f, filePath)

	require.Error(t, err)
	require.Nil(t, reader)
	require.NoError(t, f.Close())
}
