package reader

import (
	"compress/gzip"
	"errors"
	"github.com/dsub-io/go-open-discogs-batch/internal/progress"
	"golang.org/x/term"
	"io"
	"os"
	"strings"
)

func NewProgressGzipReadCloser(f *os.File, progressText string) (io.ReadCloser, error) {
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}

	progressReporter := progress.NewReporter(
		os.Stdout,
		os.Stderr,
		term.IsTerminal(int(os.Stderr.Fd())),
		progress.StageSourceRead,
		GetFilename(progressText),
		fileInfo.Size(),
	)
	progressReporter.Start()
	trackedSource := &progressReader{source: f, reporter: progressReporter}
	reader, err := gzip.NewReader(trackedSource)
	if err != nil {
		progressReporter.Fail(trackedSource.completedBytes)
		return nil, err
	}
	return &readCloserImpl{
		readDelegate:   reader,
		closeDelegates: []io.Closer{reader, f},
	}, nil
}

type progressReader struct {
	source         io.Reader
	reporter       *progress.Reporter
	completedBytes int64
}

func (r *progressReader) Read(payload []byte) (int, error) {
	readBytes, err := r.source.Read(payload)
	r.completedBytes += int64(readBytes)
	r.reporter.Set(r.completedBytes)
	if err != nil && !errors.Is(err, io.EOF) {
		r.reporter.Fail(r.completedBytes)
	}
	return readBytes, err
}

func GetFilename(filepath string) string {
	parts := strings.Split(filepath, string(os.PathSeparator))
	return parts[len(parts)-1]
}

type readCloserImpl struct {
	readDelegate   io.Reader
	closeDelegates []io.Closer
}

func (r *readCloserImpl) Read(p []byte) (n int, err error) {
	return r.readDelegate.Read(p)
}

func (r *readCloserImpl) Close() error {
	var closeErr error
	for _, delegate := range r.closeDelegates {
		closeErr = errors.Join(closeErr, delegate.Close())
	}
	return closeErr
}
