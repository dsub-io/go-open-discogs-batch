package reader

import (
	"bytes"
	"errors"
	"github.com/dsub-io/go-open-discogs-batch/internal/progress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"
)

var errProgressReader = errors.New("progress reader failure")

type failingProgressSource struct{}

func (failingProgressSource) Read([]byte) (int, error) {
	return 0, errProgressReader
}

func TestGetFilename(t *testing.T) {
	filename := GetFilename(path.Join("test", "this", "function", "as", "this.txt"))
	assert.Equal(t, "this.txt", filename)
}

func TestNewProgressGzipReadCloser(t *testing.T) {
	f, err := os.Open("testdata/data.gz")
	require.NoError(t, err)
	text := "test-progress-bar"
	r, err := NewProgressGzipReadCloser(f, text)
	require.NoError(t, err)

	payload := make([]byte, 64)
	n, err := r.Read(payload)
	require.NoError(t, err)
	require.Equal(t, n, 64)

	require.NoError(t, r.Close())
	require.Error(t, f.Close())
}

func TestNewProgressGzipReadCloserRejectsInvalidGzip(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "invalid.gz")
	require.NoError(t, os.WriteFile(filePath, []byte("not gzip"), 0o600))
	f, err := os.Open(filePath)
	require.NoError(t, err)

	reader, err := NewProgressGzipReadCloser(f, filePath)

	require.Error(t, err)
	require.Nil(t, reader)
	require.NoError(t, f.Close())
}

func TestNewProgressGzipReadCloserRejectsUnreadableFileMetadata(t *testing.T) {
	f, err := os.Open("testdata/data.gz")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reader, err := NewProgressGzipReadCloser(f, "closed")

	require.Error(t, err)
	require.Nil(t, reader)
}

func TestProgressReaderReportsReadFailure(t *testing.T) {
	var output bytes.Buffer
	reporter := progress.NewReporter(
		&output,
		io.Discard,
		false,
		progress.StageSourceRead,
		"broken",
		1,
	)
	reporter.Start()
	trackedReader := &progressReader{source: failingProgressSource{}, reporter: reporter}

	readBytes, err := trackedReader.Read(make([]byte, 1))

	require.Zero(t, readBytes)
	require.ErrorIs(t, err, errProgressReader)
	require.Contains(t, output.String(), `"state":"failed"`)
}
