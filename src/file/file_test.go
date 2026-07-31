package file

import (
	"bytes"
	"errors"
	"github.com/dsub-io/go-open-discogs-batch/internal/test/resource"
	"github.com/dsub-io/go-open-discogs-batch/internal/testserver"
	"github.com/stretchr/testify/assert"
	"net/http"
	"os"
	"testing"
	"time"
)

func Test_handlerImpl_Checksum(t *testing.T) {
	type args struct {
		filepath string
		checksum string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "returns no error when checksum is valid",
			args: args{
				filepath: "testdata/test.xml",
				checksum: "69718470e15145cf586db15389bb2bf81b4cf4ee179aa6c0dd61afaf17d56b3d",
			},
			wantErr: false,
		},
		{
			name: "returns err when checksum is invalid",
			args: args{
				filepath: "testdata/test.xml",
				checksum: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace22fcde9",
			},
			wantErr: true,
		},
		{
			name: "returns err when checksum is odd",
			args: args{
				filepath: "testdata/test.xml",
				checksum: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace22fcde",
			},
			wantErr: true,
		},
		{
			name: "returns err when file not exists",
			args: args{
				filepath: "testdata/unobtanium.xml",
				checksum: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace22fcde9",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handlerImpl{}
			if err := h.Checksum(tt.args.filepath, tt.args.checksum); (err != nil) != tt.wantErr {
				t.Errorf("Checksum() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_handlerImpl_Copy(t *testing.T) {
	f, _ := os.Create("testdata/tmp.txt")
	_ = f.Close()
	defer func() { _ = os.Remove("testdata/tmp.txt") }()
	type args struct {
		source     string
		target     string
		targetPerm os.FileMode
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "returns no error when success",
			args: args{
				source:     "testdata/test.xml",
				target:     "testdata/copy_test_dest.txt",
				targetPerm: 0644,
			},
			wantErr: false,
		},
		{
			name: "returns error when fail",
			args: args{
				source:     "testdata/none.txt",
				target:     "testdata/copy_test_dest.txt",
				targetPerm: 0644,
			},
			wantErr: true,
		},
		{
			name: "returns error when dst already exists",
			args: args{
				source:     "testdata/test.xml",
				target:     "testdata/tmp.txt",
				targetPerm: 0644,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			defer func() { _ = os.Remove("testdata/copy_test_dest.txt") }()
			h := &handlerImpl{}
			var err error
			if err = h.Copy(tt.args.source, tt.args.target, tt.args.targetPerm); (err != nil) != tt.wantErr {
				t.Errorf("Copy() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil { // on success
				compareFilesByBytes(t, tt.args.source, tt.args.target)
			}
		})
	}
}

func Test_handlerImpl_Delete(t *testing.T) {
	_ = os.WriteFile("testdata/some_file", []byte("testdata"), 0644)
	type args struct {
		filepath string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "returns no error when file not exists",
			args:    args{"testdata/not_exist.txt"},
			wantErr: false,
		},
		{
			name:    "returns no error when file successfully deleted",
			args:    args{"testdata/some_file"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handlerImpl{}
			var err error
			if err = h.Delete(tt.args.filepath); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil { // on success
				_, statErr := os.Stat(tt.args.filepath)
				assert.True(t, errors.Is(statErr, os.ErrNotExist)) // always os.ErrNotExist when no such file.
			}
		})
	}
}

func Test_handlerImpl_Fetch(t *testing.T) {
	expected := string(resource.Read("testdata/test.xml"))
	s := testserver.NewServerWithStaticResponse(expected)
	defer s.Close()

	failServer := testserver.NewServer(
		func(requests []*testserver.HttpRequest, w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Length", "50")
			_, _ = w.Write([]byte("test"))
		})
	defer failServer.Close()

	slowServer := testserver.NewServer(
		func(requests []*testserver.HttpRequest, w http.ResponseWriter, r *http.Request) {
			<-time.After(time.Millisecond * 200)
			_, _ = w.Write([]byte(expected))
		})

	type args struct {
		uri      string
		filepath string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "return no error when success",
			args: args{
				uri:      s.URL,
				filepath: "testdata/fetch_test_file.txt",
			},
			wantErr: false,
		},
		{
			name: "returns err when failure EOF",
			args: args{
				uri:      failServer.URL,
				filepath: "testdata/fetch_test_file.txt",
			},
			wantErr: true,
		},
		{
			name: "returns err when 404",
			args: args{
				uri:      "somewhere_not_exist",
				filepath: "testdata/fetch_test_fail.txt",
			},
			wantErr: true,
		},
		{
			name: "handles delayed response",
			args: args{
				uri:      slowServer.URL,
				filepath: "testdata/fetch_test_file.xml",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handlerImpl{}
			var err error
			defer func() { _ = os.Remove(tt.args.filepath) }()
			if err = h.Fetch(tt.args.uri, tt.args.filepath); (err != nil) != tt.wantErr {
				t.Errorf("Fetch() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil { // on success
				compareFilesByBytes(t, tt.args.filepath, "testdata/test.xml")
			}
		})
	}
}

func compareFilesByBytes(t *testing.T, paths ...string) {
	if len(paths) <= 1 {
		return
	}
	results := make([][]byte, 0)
	for i := range paths {
		v, err := os.ReadFile(paths[i])
		if err != nil {
			t.Errorf("failed to test comparison: %+v", err)
			return
		}
		results = append(results, v)
		if i > 0 && results[i-1] != nil {
			if !bytes.Equal(results[i], results[i-1]) {
				t.Errorf("%+v and %+v content not equal", paths[i], paths[i-1])
			}
		}
	}
}

func Test_handlerImpl_FetchAndCheck(t *testing.T) {
	successServer := testserver.NewServer(func(requests []*testserver.HttpRequest, w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(resource.Read("testdata/test.xml"))
	})
	defer successServer.Close()

	failServer := testserver.NewServer(func(requests []*testserver.HttpRequest, w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Length", "3326")
		_, _ = w.Write(resource.Read("testdata/test.xml"))
	})
	defer failServer.Close()

	type args struct {
		uri      string
		filepath string
		checksum string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "return no err when success",
			args: args{
				uri:      successServer.URL,
				filepath: "testdata/fetch_then_checksum.txt",
				checksum: "69718470e15145cf586db15389bb2bf81b4cf4ee179aa6c0dd61afaf17d56b3d",
			},
			wantErr: false,
		},
		{
			name: "return err when checksum fails",
			args: args{
				uri:      successServer.URL,
				filepath: "testdata/fetch_then_checksum.txt",
				checksum: "b94d27b9934d3e08a52e52d7da7deefac484efe37a5380ee9088f7ace2efcde9",
			},
			wantErr: true,
		},
		{
			name: "return err when EOF",
			args: args{
				uri:      failServer.URL,
				filepath: "testdata/fetch_then_checksum.txt",
				checksum: "69718470e15145cf586db15389bb2bf81b4cf4ee179aa6c0dd61afaf17d56b3d",
			},
			wantErr: true,
		},
		{
			name: "return err when 404",
			args: args{
				uri:      "none",
				filepath: "testdata/fetch_then_checksum.txt",
				checksum: "69718470e15145cf586db15389bb2bf81b4cf4ee179aa6c0dd61afaf17d56b3d",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler()
			if err := h.FetchAndCheck(tt.args.uri, tt.args.filepath, tt.args.checksum); (err != nil) != tt.wantErr {
				t.Errorf("FetchAndCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_handlerImpl_Write(t *testing.T) {
	openedFilePath := "testdata/test_write_opened_file.txt"
	f, _ := os.OpenFile(openedFilePath, os.O_RDONLY, 0644)
	defer func() {
		_ = f.Close()
		_ = os.Remove(openedFilePath)
	}()
	type args struct {
		filepath string
		content  []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "returns no err when file not exists",
			args: args{
				filepath: "testdata/write_test_file.txt",
				content:  []byte("hello world!"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handlerImpl{}
			if err := h.Write(tt.args.filepath, tt.args.content, 0644); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			} else if err == nil {
				defer func() { _ = os.Remove(tt.args.filepath) }()
			}
		})
	}
}

type testFileReader struct {
	path string
	res  []byte
	err  error
}

func (t testFileReader) Exists(path string) (bool, error) {
	panic("no-op")
}

func (t testFileReader) ReadFile(path string) ([]byte, error) {
	if t.path == path {
		return t.res, nil
	} else {
		return nil, t.err
	}
}

func Test_handlerImpl_Read(t *testing.T) {
	type fields struct {
		reader Reader
	}
	type args struct {
		filepath string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr error
	}{
		{
			name: "reads file",
			fields: fields{reader: testFileReader{
				path: "test/path",
				res:  []byte("test"),
				err:  nil,
			}},
			args:    args{"test/path"},
			want:    []byte("test"),
			wantErr: nil,
		},
		{
			name: "returns error",
			fields: fields{reader: testFileReader{
				path: "test/path",
				res:  nil,
				err:  os.ErrNotExist,
			}},
			args:    args{"somewhere"},
			want:    nil,
			wantErr: os.ErrNotExist,
		},
		{
			name:    "reads actual file",
			fields:  fields{reader: &fileReaderImpl{}},
			args:    args{"testdata/test.xml"},
			want:    resource.Read("testdata/test.xml"),
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handlerImpl{
				reader: tt.fields.reader,
			}
			got, err := h.Read(tt.args.filepath)
			assert.Equal(t, tt.wantErr, err)
			assert.Equalf(t, tt.want, got, "Read(%v)", tt.args.filepath)
		})
	}
}

func TestNewHandler(t *testing.T) {
	tests := []struct {
		name string
		want Handler
	}{
		{
			name: "creates handler",
			want: &handlerImpl{&fileReaderImpl{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewHandler(), "NewHandler()")
		})
	}
}
