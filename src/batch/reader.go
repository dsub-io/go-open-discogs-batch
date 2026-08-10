package batch

import (
	"fmt"
	"io"
	"os"

	"github.com/dsub-io/go-open-discogs-batch/src/reader"
)

func newReadCloser(filepath string, progressBarText string) (io.ReadCloser, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("open import file %s: %w", filepath, err)
	}
	readCloser, err := reader.NewProgressBarGzipReadCloser(file, progressBarText)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open gzip import file %s: %w", filepath, err)
	}
	return readCloser, nil
}
