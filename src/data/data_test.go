package data

import (
	"bytes"
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
	"path/filepath"
	"strconv"
	"strings"
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

func TestChecksumStore(t *testing.T) {
	t.Run("must save on valid line", func(t *testing.T) {
		store := newChecksumStore()
		tTime, _ := time.Parse("20060102", "20080309")
		for _, v := range validChecksumTextSliceSample {
			store.addText(v)
		}
		values := store.snapshot()
		assert.Len(t, values[tTime], 3)
		for i, v := range []string{"artists", "labels", "releases"} {
			vv, ok := values[tTime][v]
			assert.True(t, ok)
			assert.Contains(t, validChecksumTextSliceSample[i], vv)
		}
	})
	t.Run("must not save invalid line", func(t *testing.T) {
		store := newChecksumStore()
		store.addText("")
		assert.Zero(t, len(store.snapshot()))
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
	t.Run("must reject invalid URI", func(t *testing.T) {
		_, err := PopulateFromUri()(context.Background(), &Data{Uri: "invalid"})
		require.Error(t, err)
	})
	t.Run("must reject invalid date", func(t *testing.T) {
		_, err := PopulateFromUri()(context.Background(), &Data{
			Uri: "data/2008/discogs_20081409_releases.xml.gz",
		})
		require.ErrorContains(t, err, "invalid dump date")
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

type observableClientStub struct {
	observable rxgo.Observable
}

func (c observableClientStub) Get(context.Context, string) rxgo.Observable {
	return c.observable
}

type signalingClientStub struct {
	called chan<- struct{}
}

func (c signalingClientStub) Get(context.Context, string) rxgo.Observable {
	close(c.called)
	return rxgo.Never()
}

func (c clientStub) Get(_ context.Context, _ string) rxgo.Observable {
	if c.pl != nil {
		p := rxgo.Producer(func(ctx context.Context, next chan<- rxgo.Item) {
			next <- rxgo.Of(c.pl)
		})
		return rxgo.Create([]rxgo.Producer{p})
	}
	p := rxgo.Producer(func(_ context.Context, next chan<- rxgo.Item) {
		next <- rxgo.Error(c.err)
	})
	return rxgo.Create([]rxgo.Producer{p})
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
		v, err := DispatchChecksumFetch(newChecksumStore())(context.Background(), dump)
		assert.NoError(t, err)
		assert.Equal(t, dump, v.(*Data))
	})
	t.Run("must return fetch error", func(t *testing.T) {
		fetchErr := errors.New("checksum fetch failed")
		getClient = getClientStub(nil, fetchErr)
		dump := &Data{TargetType: "checksum", Uri: "checksum.txt"}
		_, err := DispatchChecksumFetch(newChecksumStore())(context.Background(), dump)
		require.ErrorIs(t, err, fetchErr)
	})
	t.Run("must reject an empty response", func(t *testing.T) {
		getClient = func() client.Client { return observableClientStub{observable: rxgo.Empty()} }
		dump := &Data{TargetType: "checksum", Uri: "checksum.txt"}
		_, err := DispatchChecksumFetch(newChecksumStore())(context.Background(), dump)
		require.ErrorContains(t, err, "response closed")
	})
	t.Run("must reject a non-byte response", func(t *testing.T) {
		getClient = func() client.Client {
			return observableClientStub{observable: rxgo.Just("invalid")()}
		}
		dump := &Data{TargetType: "checksum", Uri: "checksum.txt"}
		_, err := DispatchChecksumFetch(newChecksumStore())(context.Background(), dump)
		require.ErrorContains(t, err, "not a byte payload")
	})
	t.Run("must honor cancellation before requesting", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		dump := &Data{TargetType: "checksum", Uri: "checksum.txt"}
		_, err := DispatchChecksumFetch(newChecksumStore())(ctx, dump)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("must honor cancellation while awaiting response", func(t *testing.T) {
		called := make(chan struct{})
		getClient = func() client.Client { return signalingClientStub{called: called} }
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-called
			cancel()
		}()
		dump := &Data{TargetType: "checksum", Uri: "checksum.txt"}
		_, err := DispatchChecksumFetch(newChecksumStore())(ctx, dump)
		require.ErrorIs(t, err, context.Canceled)
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
	t.Run("must reject a missing data checksum", func(t *testing.T) {
		pt, _ := time.Parse("20060102", "20190301")
		dump := &Data{GeneratedAt: pt, TargetType: "artists"}
		_, err := SetChecksumValues(map[time.Time]map[string]string{})(context.Background(), dump)
		require.ErrorContains(t, err, "checksum not found")
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
	t.Run("propagates source and payload errors", func(t *testing.T) {
		expected := errors.New("fixture")
		result := <-ParseDumpModel(context.Background())(rxgo.Error(expected)).Observe()
		require.ErrorIs(t, result.E, expected)

		result = <-ParseDumpModel(context.Background())(rxgo.Of("invalid")).Observe()
		require.ErrorContains(t, result.E, "not a byte payload")
	})
}

type RepositoryStub struct {
	items    []*Data
	batchErr error
}

func (r *RepositoryStub) BatchInsert(data []*Data) (int, error) {
	if r.batchErr != nil {
		return 0, r.batchErr
	}
	r.items = data
	return len(data), nil
}

func (r *RepositoryStub) FindByYearMonthType(
	year, month, typ string,
) (*opendiscogsmodel.DiscogsDump, error) {
	typ = strings.TrimSuffix(typ, "s")
	for _, v := range r.items {
		if strings.TrimSuffix(v.TargetType, "s") != typ {
			continue
		}
		if len(month) == 1 { // padding
			month = " " + month
		}
		if strings.TrimSuffix(v.TargetType, "s") == typ && v.GeneratedAt.Format("200601") == year+month {
			return v.Dump(), nil
		}
	}
	return nil, errors.New("record not found")
}

func (r *RepositoryStub) FindLatestByType(typ string) (*opendiscogsmodel.DiscogsDump, error) {
	typ = strings.TrimSuffix(typ, "s")
	var latest *Data
	for _, item := range r.items {
		if strings.TrimSuffix(item.TargetType, "s") != typ {
			continue
		}
		if latest == nil || item.GeneratedAt.After(latest.GeneratedAt) {
			latest = item
		}
	}
	if latest == nil {
		return nil, errors.New("record not found")
	}
	return latest.Dump(), nil
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
		updateCount, err := UpdateData(context.Background(), repo, 2)
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

func TestUpdateDataRejectsNonPositiveMaxWorkers(t *testing.T) {
	_, err := UpdateData(context.Background(), &RepositoryStub{}, 0)
	require.ErrorContains(t, err, "max-workers must be a positive integer")
}

func TestUpdateDataPropagatesListingFailure(t *testing.T) {
	server := testserver.NewServer(func(
		requests []*testserver.HttpRequest,
		w http.ResponseWriter,
		r *http.Request,
	) {
		http.Error(w, "fixture", http.StatusInternalServerError)
	})
	defer server.Close()
	originalURL := DiscogsS3BaseUrl
	t.Cleanup(func() { DiscogsS3BaseUrl = originalURL })
	DiscogsS3BaseUrl = server.URL

	updated, err := UpdateData(context.Background(), &RepositoryStub{}, 1)
	require.Equal(t, -1, updated)
	require.Error(t, err)
}

func TestCatalogUpdateCount(t *testing.T) {
	expected := errors.New("fixture")
	updated, err := catalogUpdateCount(rxgo.Error(expected))
	require.Zero(t, updated)
	require.ErrorIs(t, err, expected)

	updated, err = catalogUpdateCount(rxgo.Of("not-a-count"))
	require.Zero(t, updated)
	require.ErrorContains(t, err, "did not contain a count")

	updated, err = catalogUpdateCount(rxgo.Of(7))
	require.NoError(t, err)
	require.Equal(t, 7, updated)
}

func TestUpdateSelectedDataBoundsChecksumRequests(t *testing.T) {
	listing := resource.Read("testdata/update-data-test.xml")
	server := testserver.NewServer(func(requests []*testserver.HttpRequest, w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("download") == "data/2008/discogs_20080309_CHECKSUM.txt" {
			_, _ = w.Write([]byte(`8c40390a3e07b60e4eaa51dfb665a20a41a5ffc337644fb4420c6adea8ed8f50 *discogs_20080309_artists.xml.gz
36772fdcfd019c995fb16f0020a8876df96f522f76de1acfa632299255664e59 *discogs_20080309_labels.xml.gz
e0e22f8501c2013eda69071a16e35ff785c0a135dee009fe2b67349f907709eb *discogs_20080309_releases.xml.gz
`))
			return
		}
		_, _ = w.Write(listing)
	})
	defer server.Close()

	s3Origin, dataOrigin := DiscogsS3BaseUrl, DiscogsDataBaseURL
	defer func() {
		DiscogsS3BaseUrl = s3Origin
		DiscogsDataBaseURL = dataOrigin
	}()
	DiscogsS3BaseUrl = server.URL + "/"
	DiscogsDataBaseURL = server.URL + "/"
	repo := &RepositoryStub{}

	updated, err := UpdateSelectedData(
		context.Background(),
		repo,
		[]string{"artist", "label", "release"},
		"",
	)

	require.NoError(t, err)
	require.Equal(t, 3, updated)
	require.Len(t, repo.items, 3)
	require.Len(t, server.Requests(), 2, "one listing plus one shared-date checksum request")
}

func TestUpdateSelectedDataErrorBoundaries(t *testing.T) {
	listing := resource.Read("testdata/update-data-test.xml")
	tests := []struct {
		name            string
		listingResponse []byte
		checksumStatus  int
		checksumBody    string
		want            string
	}{
		{
			name:            "missing checksum manifest",
			listingResponse: bytes.Replace(listing, checksumContents(listing), nil, 1),
			checksumStatus:  http.StatusOK,
			want:            "checksum manifest not found",
		},
		{
			name:            "checksum fetch failure",
			listingResponse: listing,
			checksumStatus:  http.StatusInternalServerError,
			want:            "returned 500",
		},
		{
			name:            "missing entity checksum",
			listingResponse: listing,
			checksumStatus:  http.StatusOK,
			checksumBody:    validChecksumTextSliceSample[1],
			want:            "checksum not found",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := testserver.NewServer(func(
				requests []*testserver.HttpRequest,
				w http.ResponseWriter,
				r *http.Request,
			) {
				if r.URL.Query().Has("download") {
					w.WriteHeader(test.checksumStatus)
					_, _ = w.Write([]byte(test.checksumBody))
					return
				}
				_, _ = w.Write(test.listingResponse)
			})
			defer server.Close()
			originalS3URL, originalDataURL := DiscogsS3BaseUrl, DiscogsDataBaseURL
			t.Cleanup(func() {
				DiscogsS3BaseUrl = originalS3URL
				DiscogsDataBaseURL = originalDataURL
			})
			DiscogsS3BaseUrl = server.URL
			DiscogsDataBaseURL = server.URL

			updated, err := UpdateSelectedData(
				context.Background(),
				&RepositoryStub{},
				[]string{"artist"},
				"",
			)
			require.Equal(t, -1, updated)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func checksumContents(listing []byte) []byte {
	start := bytes.Index(listing, []byte("    <Contents>"))
	if start < 0 {
		return nil
	}
	end := bytes.Index(listing[start:], []byte("    </Contents>"))
	if end < 0 {
		return nil
	}
	return listing[start : start+end+len("    </Contents>")]
}

func TestUpdateSelectedDataPropagatesListingFailure(t *testing.T) {
	server := testserver.NewServer(func(
		requests []*testserver.HttpRequest,
		w http.ResponseWriter,
		r *http.Request,
	) {
		http.Error(w, "fixture", http.StatusInternalServerError)
	})
	defer server.Close()
	originalURL := DiscogsS3BaseUrl
	t.Cleanup(func() { DiscogsS3BaseUrl = originalURL })
	DiscogsS3BaseUrl = server.URL

	updated, err := UpdateSelectedData(context.Background(), &RepositoryStub{}, []string{"artist"}, "")
	require.Equal(t, -1, updated)
	require.Error(t, err)
}

func TestSelectRefreshCandidatesHonorsMonthAndLatest(t *testing.T) {
	items := []*Data{
		{TargetType: "artists", GeneratedAt: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)},
		{TargetType: "artists", GeneratedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)},
		{TargetType: "labels", GeneratedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)},
	}
	candidates := selectRefreshCandidates(items, []string{"artist"}, "2026-06")
	require.Len(t, candidates, 1)
	require.Equal(t, time.June, candidates[0].GeneratedAt.Month())
}

func TestFetchBytesErrorBoundaries(t *testing.T) {
	originalClient := getClient
	t.Cleanup(func() { getClient = originalClient })
	expected := errors.New("fixture")

	getClient = getClientStub(nil, expected)
	_, err := fetchBytes(context.Background(), "fixture")
	require.ErrorIs(t, err, expected)

	getClient = func() client.Client {
		return observableClientStub{observable: rxgo.Just("invalid")()}
	}
	_, err = fetchBytes(context.Background(), "fixture")
	require.ErrorContains(t, err, "unexpected response body")

	getClient = func() client.Client { return observableClientStub{observable: rxgo.Empty()} }
	_, err = fetchBytes(context.Background(), "fixture")
	require.ErrorContains(t, err, "empty response")
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
entities:
  - artist
  - label
  - master
  - release
dump-month: 2022-10
data-dir: testdata
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
entities:
  - artist
dump-month: 2010-10
data-dir: testdata
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
entities:
  - artist
dump-month: 2010-10
data-dir: testdata/
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

func TestResolveImportPlanDoesNotDownload(t *testing.T) {
	server := testserver.NewServer(func(
		requests []*testserver.HttpRequest,
		w http.ResponseWriter,
		r *http.Request,
	) {
		t.Fatalf("resolve import plan must not request %s", r.URL.String())
	})
	defer server.Close()
	origin := DiscogsS3BaseUrl
	defer func() { DiscogsS3BaseUrl = origin }()
	DiscogsS3BaseUrl = server.URL + "/"

	dataDirectory := filepath.Join(t.TempDir(), "not-created")
	config := koanf.New(".")
	require.NoError(t, config.Set("entities", []string{"artist"}))
	require.NoError(t, config.Set("dump-month", "2010-10"))
	require.NoError(t, config.Set("data-dir", dataDirectory))
	repository := &RepositoryStub{}
	_, err := repository.BatchInsert([]*Data{
		{
			ETag:        "artist-2010-10",
			GeneratedAt: time.Date(2010, 10, 1, 0, 0, 0, 0, time.UTC),
			Checksum:    strings.Repeat("a", 64),
			TargetType:  "artist",
			Uri:         "data/2010/discogs_20101001_artists.xml.gz",
		},
	})
	require.NoError(t, err)

	plan, err := ResolveImportPlan(config, repository)

	require.NoError(t, err)
	require.Len(t, plan.Dumps, 1)
	require.Contains(t, plan.Resources["artists"], "discogs_20101001_artists.xml.gz")
	require.NoDirExists(t, dataDirectory)
	require.Empty(t, server.Requests())
}

func TestResolveImportPlanUsesLatestDump(t *testing.T) {
	config := koanf.New(".")
	require.NoError(t, config.Set("entities", []string{"artist"}))
	require.NoError(t, config.Set("data-dir", t.TempDir()))
	repository := &RepositoryStub{items: []*Data{
		{
			GeneratedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			TargetType:  "artists",
			Uri:         "data/2026/discogs_20260701_artists.xml.gz",
		},
	}}

	plan, err := ResolveImportPlan(config, repository)
	require.NoError(t, err)
	require.Len(t, plan.Dumps, 1)
	require.Contains(t, plan.Resources["artists"], "discogs_20260701_artists.xml.gz")
}

func TestFetchImportResourcesRejectsInvalidPlanAndDirectory(t *testing.T) {
	dump := &opendiscogsmodel.DiscogsDump{
		EntityType:     "artist",
		URI:            "data/2026/discogs_20260701_artists.xml.gz",
		ChecksumSHA256: strings.Repeat("0", 64),
	}
	require.ErrorContains(t, FetchImportResources(context.Background(), &ImportPlan{
		Resources: map[string]string{},
		Dumps:     []*opendiscogsmodel.DiscogsDump{dump},
	}), "missing artist import resource path")

	blockingFile := filepath.Join(t.TempDir(), "blocking-file")
	require.NoError(t, os.WriteFile(blockingFile, []byte("fixture"), 0600))
	require.ErrorContains(t, FetchImportResources(context.Background(), &ImportPlan{
		Resources: map[string]string{
			"artists": filepath.Join(blockingFile, "child", "artists.xml.gz"),
		},
		Dumps: []*opendiscogsmodel.DiscogsDump{dump},
	}), "create artist import directory")
}
