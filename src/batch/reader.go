package batch

import (
	"io"
	"os"

	"github.com/dsub-io/go-open-discogs-batch/src/reader"
)

func newReadCloser(filepath string, progressBarText string) io.ReadCloser {
	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	readCloser, err := reader.NewProgressBarGzipReadCloser(file, progressBarText)
	if err != nil {
		_ = file.Close()
		panic(err)
	}
	return readCloser
}
