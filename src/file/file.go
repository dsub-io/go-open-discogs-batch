package file

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/cavaliergopher/grab/v3"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/schollz/progressbar/v3"
	"io"
	"os"
	"time"
)

type Handler interface {
	// Copy file from source to destination by chunks of 32KB.
	Copy(srcPath, dstPath string, targetPerm os.FileMode) error
	// Delete deletes file. If there is an error, it will be of types *os.PathError.
	Delete(filepath string) error
	// Checksum validates a file from given path.
	// Returns error only if it cannot read or locate the file, or the checksum failed.
	Checksum(filepath string, checksum string) error
	// Fetch will retrieve file from given uri
	Fetch(uri string, filepath string) error
	// FetchAndCheck calls Fetch to retrieve file, while checking the checksum to validate incoming file.
	// The file will be deleted if checksum fails.
	FetchAndCheck(uri string, filepath string, checksum string) error
	// FetchAndCheckContext is FetchAndCheck with cancellation support.
	FetchAndCheckContext(context.Context, string, string, string) error
	// Write writes byte array content to given filepath and permission.
	Write(filepath string, content []byte, perm os.FileMode) error
	// Read reads byte array content from given filepath. This works as identical delegation to os.ReadFile.
	Read(filepath string) ([]byte, error)
	// Exists checks filepath to see if exists
	Exists(filepath string) (bool, error)
}

func NewHandler() Handler {
	return &handlerImpl{reader: &fileReaderImpl{}}
}

type handlerImpl struct {
	reader Reader
}

type Reader interface {
	ReadFile(path string) ([]byte, error)
	Exists(path string) (bool, error)
}

type fileReaderImpl struct{}

func (f *fileReaderImpl) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (f *fileReaderImpl) Exists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (h *handlerImpl) Read(filepath string) ([]byte, error) {
	return h.reader.ReadFile(filepath)
}

func (h *handlerImpl) Exists(filepath string) (bool, error) {
	return h.reader.Exists(filepath)
}

func (h *handlerImpl) Copy(source, target string, targetPerm os.FileMode) error {
	var (
		src *os.File
		dst *os.File
		err error
	)
	if src, err = os.OpenFile(source, os.O_RDONLY, 0755); err != nil {
		return err
	}
	defer src.Close()
	if dst, err = os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666); err != nil {
		_ = src.Close()
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	return err
}

func (h *handlerImpl) Delete(filepath string) error {
	if err := os.Remove(filepath); err != nil && errors.Is(err, os.ErrNotExist) {
		return nil
	} else {
		return err
	}
}

func (h *handlerImpl) Checksum(filepath string, checksum string) error {
	// get decoded checksum
	sum, err := h.getDecodedChecksum(checksum)
	if err != nil {
		return err
	}

	// open file
	var f *os.File
	if f, err = os.OpenFile(filepath, os.O_RDONLY, 0644); err != nil {
		return err
	}
	defer f.Close()

	// check
	hash := sha256.New()
	if _, err = io.Copy(hash, f); err != nil {
		return err
	}

	if !bytes.Equal(sum, hash.Sum(nil)) {
		return errors.New("checksum not match")
	}

	return nil
}

func (h *handlerImpl) Fetch(uri string, filepath string) error {
	req, err := h.newRequest(uri, filepath)
	if err != nil {
		return err
	}
	return h.execGrabReq(req)
}

func (h *handlerImpl) FetchAndCheck(uri string, filepath string, checksum string) error {
	return h.FetchAndCheckContext(context.Background(), uri, filepath, checksum)
}

func (h *handlerImpl) FetchAndCheckContext(
	ctx context.Context,
	uri string,
	filepath string,
	checksum string,
) error {
	var (
		sum      []byte
		err      error
		found    bool
		filename = reader.GetFilename(filepath)
	)

	found, err = h.Exists(filepath)

	if err != nil {
		return err
	} else if found {
		fmt.Printf("found %+v. testing checksum...\n", filename)
		if err = h.Checksum(filepath, checksum); err == nil {
			fmt.Printf("%+v checksum done. skipping fetch\n", filename)
			return nil
		}
		fmt.Printf("%+v failed checksum. deleting...\n", filename)
		if err = h.Delete(filepath); err != nil {
			return err
		}
	}
	fmt.Printf("fetching %+v...\n", filename)
	// prepare checksum
	if sum, err = h.getDecodedChecksum(checksum); err != nil {
		return err
	}
	// grab request with checksum
	req, err := h.newRequestWithChecksum(uri, filepath, sum)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	// request
	return h.execGrabReq(req)
}

func (h *handlerImpl) getDecodedChecksum(checksum string) ([]byte, error) {
	sum, err := hex.DecodeString(checksum)
	if err != nil {
		return nil, fmt.Errorf("failed to decode checksum %w", err)
	}
	return sum, nil
}

func (h *handlerImpl) Write(filepath string, content []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(content)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

func (h *handlerImpl) newRequest(uri, filepath string) (*grab.Request, error) {
	req, err := grab.NewRequest(filepath, uri)
	if err != nil {
		return nil, fmt.Errorf("create download request: %w", err)
	}
	return req, nil
}

func (h *handlerImpl) newRequestWithChecksum(
	uri string,
	filepath string,
	checksum []byte,
) (*grab.Request, error) {
	req, err := h.newRequest(uri, filepath)
	if err != nil {
		return nil, err
	}
	req.SetChecksum(sha256.New(), checksum, true)
	return req, nil
}

func (h *handlerImpl) execGrabReq(req *grab.Request) error {
	client := grab.NewClient()

	// fire
	begin := time.Now()
	resp := client.Do(req)

	// monitor
	pb := getProgressBar(reader.GetFilename(req.Filename), resp.Size())
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
Loop:
	for {
		select {
		case <-ticker.C:
			_ = pb.Set64(resp.BytesComplete())
		case <-resp.Done:
			_ = pb.Finish()
			break Loop
		}
	}
	fmt.Println()
	if err := resp.Err(); err != nil {
		defer func() { _ = os.Remove(req.Filename) }()
		return fmt.Errorf("failed to download %w", err)
	}
	fmt.Printf("download completed for %+v (took: %.2fs)\n", req.Filename, time.Since(begin).Seconds())
	return nil
}

func getProgressBar(filename string, size int64) *progressbar.ProgressBar {
	pb := progressbar.NewOptions64(size,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription(fmt.Sprintf("[reset]writing %+v...", filename)),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
	return pb
}
