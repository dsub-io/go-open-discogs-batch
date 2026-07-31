package data

import (
	"context"
	"errors"
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/internal/test/resource"
	"github.com/dsub-io/go-open-discogs-batch/internal/testserver"
	"github.com/dsub-io/go-open-discogs-batch/src/client"
	"github.com/dsub-io/go-open-discogs-batch/src/file"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/reactivex/rxgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

var validChecksumTextSliceSample = []string{
	"8c40390a3e07b60e4eaa51dfb665a20a41a5ffc337644fb4420c6adea8ed8f50 *discogs_20080309_artists.xml.gz",
	"36772fdcfd019c995fb16f0020a8876df96f522f76de1acfa632299255664e59 *discogs_20080309_labels.xml.gz",
	"e0e22f8501c2013eda69071a16e35ff785c0a135dee009fe2b67349f907709eb *discogs_20080309_releases.xml.gz",
}

func Test_parseChecksumTextLine(t *testing.T) {
	t.Run("must parse valid checksum", func(t *testing.T) {
		for _, v := range validChecksumTextSliceSample {
			chk, ok := parseChecksumTextLine(v)
			assert.True(t, ok)
			assert.Equal(t, chk.gen.Month(), time.Month(3))
			assert.Equal(t, chk.gen.Day(), 9)
			assert.Equal(t, chk.gen.Year(), 2008)
			assert.Contains(t, v, chk.typ)
		}
	})
	t.Run("must not parse empty or invalid format", func(t *testing.T) {
		v := []string{
			"", "   ", " +_", "hello",
			"e0e22f8501c2013eda69071a16e35ff785c0a135dee009fe2b67349f907709eb *discogs_20081409_releases.xml.gz",
		}
		for i := range v {
			chk, ok := parseChecksumTextLine(v[i])
			assert.False(t, ok)
			assert.Equal(t, chk.chk, "")
			assert.Equal(t, chk.typ, "")
			assert.Equal(t, chk.gen.Year(), 1)
		}
	})
}

func Test_storeChecksum(t *testing.T) {
	t.Run("must save on valid line", func(t *testing.T) {
		t.Cleanup(func() {
			for k := range checksumMap {
				delete(checksumMap, k)
			}
		})
		tTime, _ := time.Parse("20060102", "20080309")
		for _, v := range validChecksumTextSliceSample {
			storeChecksum(v)
		}
		assert.Len(t, checksumMap[tTime], 3)
		for i, v := range []string{"artists", "labels", "releases"} {
			vv, ok := checksumMap[tTime][v]
			assert.True(t, ok)
			assert.Contains(t, validChecksumTextSliceSample[i], vv)
		}
	})
	t.Run("must not save invalid line", func(t *testing.T) {
		storeChecksum("")
		assert.Zero(t, len(checksumMap))
	})
}

func Test_parseData(t *testing.T) {
	t.Run("must readFromURI all items", func(t *testing.T) {
		for _, v := range append(make([]*Data, 0),
			&Data{Uri: "data/2008/discogs_20080309_CHECKSUM.txt"},
			&Data{Uri: "data/2008/discogs_20080309_artists.xml.gz"},
			&Data{Uri: "data/2008/discogs_20080309_labels.xml.gz"},
			&Data{Uri: "data/2008/discogs_20080309_releases.xml.gz"},
		) {
			assert.True(t, ValidUriFilter()(v))
		}
	})
	t.Run("must filter invalid item", func(t *testing.T) {
		for _, v := range append(make([]*Data, 0),
			&Data{Uri: "data/2008/discogs_20080334_CHECKSUM.txt"},
			&Data{Uri: "data/2008/discogs_20080009_artists.xml.gz"},
			&Data{Uri: "data/2008/discogs_20081509_labels.xml.gz"},
			&Data{Uri: "data/2008/discogs_00000300_releases.xml.gz"},
			&Data{Uri: "data/2008/discogs_20090310_orange.xml.gz"},
			&Data{Uri: "arrays.js"},
			&Data{Uri: "example-config.yaml"},
			&Data{Uri: "helm-values.json"},
		) {
			assert.False(t, ValidUriFilter()(v))
		}
	})
	t.Run("must filter error item", func(t *testing.T) {
		assert.False(t, ValidUriFilter()(fmt.Errorf("test error")))
	})
	t.Run("must filter nil item", func(t *testing.T) {
		assert.False(t, ValidUriFilter()(nil))
	})
}

func TestPopulateFromUri(t *testing.T) {
	t.Run("must fill date and types", func(t *testing.T) {
		d := &Data{Uri: "data/2008/discogs_20081014_releases.xml.gz"}
		r, err := PopulateFromUri()(context.Background(), d)
		assert.NoError(t, err)
		d = r.(*Data)
		pt, _ := time.Parse("20060102", "20081014")
		assert.Equal(t, pt, d.GeneratedAt)
		assert.Equal(t, "releases", d.TargetType)
	})
}

func TestNotNilFilter(t *testing.T) {
	f := NotNilFilter()
	t.Run("returns false if nil", func(t *testing.T) {
		assert.False(t, f(nil))
	})
	t.Run("returns true if not nil", func(t *testing.T) {
		assert.True(t, f(1))
	})
}

type clientStub struct {
	pl  []byte
	err error
}

func (c clientStub) Get(_ context.Context, _ string) rxgo.Observable {
	if c.pl != nil {
		p := rxgo.Producer(func(ctx context.Context, next chan<- rxgo.Item) {
			next <- rxgo.Of(c.pl)
		})
		return rxgo.Create([]rxgo.Producer{p})
	} else {
		return rxgo.Just(c.err)()
	}
}

func getClientStub(pl []byte, err error) func() client.Client {
	return func() client.Client {
		return clientStub{pl, err}
	}
}

func TestDispatchChecksumFetch(t *testing.T) {
	origin := getClient
	defer func() { getClient = origin }()
	t.Run("must save valid item", func(t *testing.T) {
		data := `8c40390a3e07b60e4eaa51dfb665a20a41a5ffc337644fb4420c6adea8ed8f50 *discogs_20080309_artists.xml.gz
36772fdcfd019c995fb16f0020a8876df96f522f76de1acfa632299255664e59 *discogs_20080309_labels.xml.gz
e0e22f8501c2013eda69071a16e35ff785c0a135dee009fe2b67349f907709eb *discogs_20080309_releases.xml.gz`
		getClient = getClientStub([]byte(data), nil)
		dump := &Data{TargetType: "checksum", Uri: ""}
		v, err := DispatchChecksumFetch()(context.Background(), dump)
		assert.NoError(t, err)
		assert.Equal(t, dump, v.(*Data))
		checksumFetchWg.Wait()
	})
}

func TestSetChecksumValues(t *testing.T) {
	t.Run("must refer values", func(t *testing.T) {
		m := make(map[time.Time]map[string]string)
		f := SetChecksumValues(m)
		pt, _ := time.Parse("20060102", "20190301")
		m[pt] = map[string]string{"artists": "test_checksum"}
		dump := &Data{
			ETag:        "",
			GeneratedAt: pt,
			Checksum:    "",
			TargetType:  "artists",
			Uri:         "",
		}
		v, err := f(context.Background(), dump)
		assert.NoError(t, err)
		assert.NotNil(t, v)
		d, ok := v.(*Data)
		assert.True(t, ok)
		assert.Equal(t, "test_checksum", d.Checksum)
	})
}

func TestGetClient(t *testing.T) {
	c := getClient()
	assert.Equal(t, client.NewClient(), c)
}

func TestParseDataModel(t *testing.T) {
	t.Run("parse all items", func(t *testing.T) {
		b, err := os.ReadFile("testdata/test.xml")
		assert.NoError(t, err)
		f := ParseDumpModel(context.Background())
		for parsed := range f(rxgo.Of(b)).Observe() {
			assert.NotNil(t, parsed)
			assert.NotNil(t, parsed.V)
			assert.NoError(t, parsed.E)
			v, ok := parsed.V.(*Data)
			assert.True(t, ok)
			assert.NotNil(t, v)
			assert.NotEmpty(t, v.Uri)
		}
	})
}

type RepositoryStub struct {
	items []*Data
}

func (r *RepositoryStub) BatchInsert(data []*Data) (int, error) {
	r.items = data
	return len(data), nil
}

func (r *RepositoryStub) FindByYearMonthType(
	year, month, typ string,
) (*opendiscogsmodel.DiscogsDump, error) {
	for _, v := range r.items {
		if v.TargetType != typ {
			continue
		}
		if len(month) == 1 { // padding
			month = " " + month
		}
		if v.TargetType == typ && v.GeneratedAt.Format("200601") == year+month {
			return v.Dump(), nil
		}
	}
	return nil, errors.New("record not found")
}

func TestUpdateData(t *testing.T) {
	data := resource.Read("testdata/update-data-test.xml")
	server := testserver.NewServer(func(requests []*testserver.HttpRequest, w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if r.URL.Query().Get("download") == "data/2008/discogs_20080309_CHECKSUM.txt" {
			_, _ = w.Write([]byte(`8c40390a3e07b60e4eaa51dfb665a20a41a5ffc337644fb4420c6adea8ed8f50 *discogs_20080309_artists.xml.gz
36772fdcfd019c995fb16f0020a8876df96f522f76de1acfa632299255664e59 *discogs_20080309_labels.xml.gz
e0e22f8501c2013eda69071a16e35ff785c0a135dee009fe2b67349f907709eb *discogs_20080309_releases.xml.gz\n`))
		} else if len(path) == 0 || path == "/" {
			_, _ = w.Write(data)
		}
	})
	defer server.Close()

	t.Run("UpdateDate updates items", func(t *testing.T) {
		s3Origin := DiscogsS3BaseUrl
		dataOrigin := DiscogsDataBaseURL
		defer func() {
			DiscogsS3BaseUrl = s3Origin
			DiscogsDataBaseURL = dataOrigin
		}()
		DiscogsS3BaseUrl = server.URL + "/"
		DiscogsDataBaseURL = server.URL + "/"
		repo := &RepositoryStub{items: make([]*Data, 0)}
		updateCount, err := UpdateData(context.Background(), repo)
		require.NoError(t, err)
		require.Equal(t, 4, updateCount)
		require.Len(t, repo.items, 4)
		for _, item := range repo.items {
			require.NotEmpty(t, item.ETag)
			require.NotEmpty(t, item.GeneratedAt)
			require.NotEmpty(t, item.TargetType)
			if item.TargetType != "checksum" {
				require.NotEmpty(t, item.Checksum)
			}
			require.NotEmpty(t, item.Uri)
		}
	})
}

func TestFetchFiles(t *testing.T) {

	h := file.NewHandler()
	server := testserver.NewServer(func(requests []*testserver.HttpRequest, w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/fetch-data-result" {
			data := resource.Read("testdata/fetch-data-test.xml")
			w.WriteHeader(200)
			w.Header().Add("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(resource.Read("testdata/fetch-data-test.xml"))
		} else {
			_, _ = w.Write([]byte("INVALID"))
		}
	})
	defer server.Close()

	t.Run("must return error when not found", func(t *testing.T) {
		t.Cleanup(func() { _ = h.Delete("testdata/fetch-data-result") })
		k := koanf.New(".")
		err := k.Load(rawbytes.Provider([]byte(`
types:
  - artist
  - label
  - master
  - release
year: 2022
month: 10
`)), yaml.Parser())
		require.NoError(t, err)
		repo := &RepositoryStub{}
		result, err := FetchFiles(k, repo)
		require.ErrorContains(t, err, "not found")
		require.Nil(t, result)
		fmt.Println(err.Error())
	})

	t.Run("must return items when valid", func(t *testing.T) {
		t.Cleanup(func() { _ = h.Delete("testdata/fetch-data-result") })
		origin := DiscogsS3BaseUrl
		defer func() { DiscogsS3BaseUrl = origin }()
		DiscogsS3BaseUrl = server.URL + "/"
		k := koanf.New(".")
		err := k.Load(rawbytes.Provider([]byte(`
types:
  - artists
year: 2010
month: 10
data: testdata
`)), yaml.Parser())
		require.NoError(t, err)
		checksum := "69718470e15145cf586db15389bb2bf81b4cf4ee179aa6c0dd61afaf17d56b3d"
		repo := &RepositoryStub{}
		insert, err := repo.BatchInsert(append(make([]*Data, 0),
			&Data{
				ETag:        "",
				GeneratedAt: time.Date(2010, 10, 1, 0, 0, 0, 0, time.UTC),
				Checksum:    checksum,
				TargetType:  "artists",
				Uri:         "fetch-data-result",
			}))
		require.NoError(t, err)
		require.Equal(t, 1, insert)

		result, err := FetchFiles(k, repo)
		require.NoError(t, err)
		require.NotNil(t, result)

		ok, err := h.Exists("testdata/fetch-data-result")
		require.True(t, ok)
		require.NoError(t, err)

		got, err := h.Read("testdata/fetch-data-result")
		require.NoError(t, err)
		expected, err := h.Read("testdata/fetch-data-test.xml")
		require.NoError(t, err)

		require.Equal(t, len(expected), len(got))
		for i := range got {
			require.Equal(t, expected[i], got[i])
		}

		require.Contains(t, result["artists"], "testdata/fetch-data-result")
	})

	t.Run("must report error when checksum failed", func(t *testing.T) {
		origin := DiscogsS3BaseUrl
		defer func() { DiscogsS3BaseUrl = origin }()
		DiscogsS3BaseUrl = server.URL + "/"
		k := koanf.New(".")
		err := k.Load(rawbytes.Provider([]byte(`
types:
  - artists
year: 2010
month: 10
data: testdata/
`)), yaml.Parser())
		require.NoError(t, err)
		checksum := "69718470e15145cf586db15389bb2bf81b4cf4ee179aa6c0dd61afaf17d56b3d"
		repo := &RepositoryStub{}
		_, _ = repo.BatchInsert(append(make([]*Data, 0),
			&Data{
				ETag:        "",
				GeneratedAt: time.Date(2010, 10, 1, 0, 0, 0, 0, time.UTC),
				Checksum:    checksum,
				TargetType:  "artists",
				Uri:         "wrong",
			}))

		result, err := FetchFiles(k, repo)
		require.ErrorContains(t, err, "checksum")
		require.Nil(t, result)
	})
}
