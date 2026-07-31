package data

import (
	"bytes"
	"context"
	"github.com/dsub-io/go-open-discogs-batch/src/client"
	"github.com/dsub-io/go-open-discogs-batch/src/file"
	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	"github.com/dsub-io/go-open-discogs-batch/src/xmlparser"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/knadh/koanf"
	"github.com/reactivex/rxgo/v2"
	"io"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"
)

var DiscogsS3BaseUrl = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/"
var DiscogsDataBaseURL = "https://data.discogs.com/"

var dumpUriPattern = regexp.MustCompile(`^data/(\d{4})/discogs_(\d{8})_(\w+).(.*)$`)
var checksumPattern = regexp.MustCompile(`^([^ ]+) +.*(\d{8})_([^.]+).*$`)
var dataTypesRegexp = regexp.MustCompile(`^(artists|labels|masters|releases|checksum)$`)

// checksumMap to collect checksum
var checksumMap = make(map[time.Time]map[string]string)

// mu locks checksumMap write
var mu = new(sync.RWMutex)

// checksumFetchWg forces execution stage to wait until checksum fetch is done
var checksumFetchWg = new(sync.WaitGroup)

func syncSave(g time.Time, t, c string) {
	mu.Lock()
	defer mu.Unlock()

	if v, ok := checksumMap[g]; ok {
		v[t] = c
	} else {
		checksumMap[g] = make(map[string]string)
		checksumMap[g][t] = c
	}
}

func storeChecksum(s string) {
	for _, line := range strings.Split(s, "\n") {
		if c, ok := parseChecksumTextLine(line); ok {
			syncSave(c.gen, c.typ, c.chk)
		}
	}
}

func parseChecksumTextLine(line string) (chk chkSumP, ok bool) {
	if len(line) == 0 {
		return chk, false
	}
	match := checksumPattern.FindStringSubmatch(line)
	if match == nil {
		return chk, false
	}
	c, g, t := match[1], match[2], match[3]
	if pt, err := time.Parse("20060102", g); err != nil {
		return chk, false
	} else {
		chk.gen = pt
		chk.chk = c
		chk.typ = t
		return chk, true
	}
}

func PopulateFromUri() func(ctx context.Context, i interface{}) (interface{}, error) {
	return func(ctx context.Context, i interface{}) (interface{}, error) {
		dump := i.(*Data)
		match := dumpUriPattern.FindStringSubmatch(dump.Uri)
		t, _ := time.Parse("20060102", match[2])
		vt := strings.ToLower(match[3])
		dump.TargetType = vt
		dump.GeneratedAt = t
		return dump, nil
	}
}

func NotNilFilter() func(i interface{}) bool {
	return func(i interface{}) bool { return i != nil }
}

// TODO: refactor

// ValidUriFilter filter items by validating URI, judged by date, types and uri pattern.
func ValidUriFilter() func(i interface{}) bool {
	return func(i interface{}) bool {
		if m, ok := i.(*Data); !ok {
			return false
		} else {
			if m := dumpUriPattern.FindStringSubmatch(m.Uri); m == nil {
				return false
			} else if _, err := time.Parse("20060102", m[2]); err != nil {
				return false
			} else {
				return isKnownType(strings.ToLower(m[3]))
			}
		}
	}
}

func isKnownType(typeStr string) bool {
	return dataTypesRegexp.MatchString(typeStr)
}

func DispatchChecksumFetch() func(context.Context, interface{}) (interface{}, error) {
	return func(ctx context.Context, i interface{}) (interface{}, error) {
		if dump := i.(*Data); dump.TargetType == "checksum" {
			checksumFetchWg.Add(1)
			go func() {
				defer checksumFetchWg.Done()
				select {
				case v := <-getClient().Get(ctx, checksumDownloadURL(dump.Uri)).Observe():
					if !v.Error() {
						storeChecksum(string(v.V.([]byte)))
					}
				case <-ctx.Done():
					return
				}
			}()
		}
		return i, nil
	}
}

func checksumDownloadURL(uri string) string {
	return DiscogsDataBaseURL + "?download=" + url.QueryEscape(uri)
}

func SetChecksumValues(m map[time.Time]map[string]string) func(ctx context.Context, i interface{}) (interface{}, error) {
	return func(ctx context.Context, i interface{}) (interface{}, error) {
		v := i.(*Data)
		v.Checksum = m[v.GeneratedAt][v.TargetType]
		return v, nil
	}
}

var getClient = func() client.Client {
	return client.NewClient()
}

type chkSumP struct {
	chk string
	gen time.Time
	typ string
}

func ParseDumpModel(ctx context.Context) func(item rxgo.Item) rxgo.Observable {
	return func(item rxgo.Item) rxgo.Observable {
		p := xmlparser.NewParser[Data]()
		buf := bytes.NewBuffer(item.V.([]byte))
		return p.Parse(ctx, xmlparser.SimpleTokenOrder(io.NopCloser(buf), "Contents"))
	}
}

func UpdateData(ctx context.Context, repo Repository) (int, error) {
	c := client.NewClient()

	items, err := c.Get(ctx, DiscogsS3BaseUrl).
		FlatMap(ParseDumpModel(ctx)).
		Filter(NotNilFilter()).
		Filter(ValidUriFilter()).
		Map(PopulateFromUri()).
		Map(DispatchChecksumFetch(), rxgo.WithCPUPool()). // NOT ordered
		ToSlice(400, rxgo.WithContext(ctx))               // known size: 777 and beyond

	if err != nil {
		return -1, err
	}

	// wait until checksum fetch is complete
	wgSig := make(chan struct{}, 1)
	go func() {
		defer close(wgSig)
		checksumFetchWg.Wait()
		wgSig <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return 0, nil
	case <-wgSig:
		break
	}

	res := <-rxgo.Just(items)().
		Map(SetChecksumValues(checksumMap)).
		Map(helper.SliceMapper[*Data]()).
		Filter(NotNilFilter()).
		Reduce(helper.SliceReducer[*Data]()).
		Map(BatchInsertItems(repo)).
		Observe()
	return res.V.(int), res.E
}

func BatchInsertItems(repo Repository) func(ctx context.Context, i interface{}) (interface{}, error) {
	return func(ctx context.Context, i interface{}) (interface{}, error) {
		data := i.([]*Data)
		return repo.BatchInsert(data)
	}
}

type ImportPlan struct {
	Resources map[string]string
	Dumps     []*opendiscogsmodel.DiscogsDump
}

func FetchImportPlan(k *koanf.Koanf, dataRepo Repository) (*ImportPlan, error) {
	plan := &ImportPlan{
		Resources: make(map[string]string),
		Dumps:     make([]*opendiscogsmodel.DiscogsDump, 0, len(k.Strings("types"))),
	}
	year, month := k.String("year"), k.String("month")
	dataRootDir := k.String("data")
	handler := file.NewHandler()
	for _, typ := range k.Strings("types") {
		d, err := dataRepo.FindByYearMonthType(year, month, typ)
		if err != nil {
			return nil, err
		}
		var (
			resourceURI = DiscogsS3BaseUrl + d.URI
			targetPath  = path.Join(dataRootDir, helper.GetLastUriSegment(d.URI))
		)
		err = handler.FetchAndCheck(resourceURI, targetPath, d.ChecksumSHA256)
		if err != nil {
			return nil, err
		}
		plan.Resources[typ] = targetPath
		plan.Dumps = append(plan.Dumps, d)
	}
	return plan, nil
}

func FetchFiles(k *koanf.Koanf, dataRepo Repository) (map[string]string, error) {
	plan, err := FetchImportPlan(k, dataRepo)
	if err != nil {
		return nil, err
	}
	return plan.Resources, nil
}
