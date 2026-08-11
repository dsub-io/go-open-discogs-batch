package reader

import (
	"compress/gzip"
	"errors"
	"github.com/schollz/progressbar/v3"
	"io"
	"os"
	"strings"
	"time"
)

func NewProgressBarGzipReadCloser(f *os.File, progressBarText string) (io.ReadCloser, error) {
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}

	pb := getProgressBar(GetFilename(progressBarText), fileInfo.Size())
	pbReader := progressbar.NewReader(f, pb)
	reader, err := gzip.NewReader(&pbReader)
	if err != nil {
		return nil, err
	}
	return &readCloserImpl{
		readDelegate:   reader,
		closeDelegates: []io.Closer{reader, f},
	}, nil
}

func getProgressBar(text string, size int64) *progressbar.ProgressBar {
	pb := progressbar.NewOptions64(size,
		progressbar.OptionEnableColorCodes(false),
		progressbar.OptionShowBytes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetElapsedTime(true),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionThrottle(time.Millisecond*250),
		progressbar.OptionSetWidth(15),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionSpinnerType(70),
		progressbar.OptionSetDescription(text),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
	return pb
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
