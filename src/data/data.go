package data

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/client"
	"github.com/dsub-io/go-open-discogs-batch/src/file"
	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	"github.com/dsub-io/go-open-discogs-batch/src/xmlparser"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/knadh/koanf"
	"github.com/reactivex/rxgo/v2"
)

var DiscogsDataBaseURL = "https://data.discogs.com/"

const dumpDateLayout = "20060102"

var dumpUriPattern = regexp.MustCompile(`^data/(\d{4})/discogs_(\d{8})_(\w+)\.(.*)$`)
var checksumPattern = regexp.MustCompile(`^([^ ]+) +.*(\d{8})_([^.]+).*$`)
var dataTypesRegexp = regexp.MustCompile(`^(artists|labels|masters|releases|checksum)$`)

type checksumStore struct {
	mu     sync.RWMutex
	values map[time.Time]map[string]string
}

func newChecksumStore() *checksumStore {
	return &checksumStore{values: make(map[time.Time]map[string]string)}
}

func (s *checksumStore) addText(text string) {
	for _, line := range strings.Split(text, "\n") {
		if checksum, ok := parseChecksumTextLine(line); ok {
			s.save(checksum)
		}
	}
}

func (s *checksumStore) save(checksum chkSumP) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if values, ok := s.values[checksum.gen]; ok {
		values[checksum.typ] = checksum.chk
		return
	}
	s.values[checksum.gen] = map[string]string{checksum.typ: checksum.chk}
}

func (s *checksumStore) snapshot() map[time.Time]map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := make(map[time.Time]map[string]string, len(s.values))
	for generatedAt, values := range s.values {
		snapshot[generatedAt] = make(map[string]string, len(values))
		for targetType, checksum := range values {
			snapshot[generatedAt][targetType] = checksum
		}
	}
	return snapshot
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
	pt, err := time.Parse(dumpDateLayout, g)
	if err != nil {
		return chk, false
	}
	chk.gen = pt
	chk.chk = c
	chk.typ = t
	return chk, true
}

func PopulateFromUri() func(_ context.Context, i interface{}) (interface{}, error) {
	return func(_ context.Context, i interface{}) (interface{}, error) {
		dump := i.(*Data)
		match := dumpUriPattern.FindStringSubmatch(dump.Uri)
		if match == nil {
			return nil, fmt.Errorf("invalid dump URI: %s", dump.Uri)
		}
		t, err := time.Parse(dumpDateLayout, match[2])
		if err != nil {
			return nil, fmt.Errorf("invalid dump date in URI %s: %w", dump.Uri, err)
		}
		vt := strings.ToLower(match[3])
		dump.TargetType = vt
		dump.GeneratedAt = t
		return dump, nil
	}
}

func NotNilFilter() func(i interface{}) bool {
	return func(i interface{}) bool { return i != nil }
}

// ValidUriFilter filter items by validating URI, judged by date, types and uri pattern.
func ValidUriFilter() func(i interface{}) bool {
	return func(i interface{}) bool {
		dump, ok := i.(*Data)
		if !ok || dump == nil {
			return false
		}
		match := dumpUriPattern.FindStringSubmatch(dump.Uri)
		if match == nil {
			return false
		}
		if _, err := time.Parse(dumpDateLayout, match[2]); err != nil {
			return false
		}
		return isKnownType(strings.ToLower(match[3]))
	}
}

func isKnownType(typeStr string) bool {
	return dataTypesRegexp.MatchString(typeStr)
}

func DispatchChecksumFetch(store *checksumStore) func(context.Context, interface{}) (interface{}, error) {
	return func(ctx context.Context, i interface{}) (interface{}, error) {
		if dump := i.(*Data); dump.TargetType == "checksum" {
			if err := ctx.Err(); err != nil {
				return i, err
			}
			select {
			case v, ok := <-getClient().Get(ctx, checksumDownloadURL(dump.Uri)).Observe():
				if !ok {
					return i, fmt.Errorf("checksum response closed for %s", dump.Uri)
				}
				if v.Error() {
					return i, v.E
				}
				body, ok := v.V.([]byte)
				if !ok {
					return i, fmt.Errorf("checksum response for %s was not a byte payload", dump.Uri)
				}
				store.addText(string(body))
			case <-ctx.Done():
				return i, ctx.Err()
			}
		}
		return i, nil
	}
}

func checksumDownloadURL(uri string) string {
	return dataDownloadURL(uri)
}

func SetChecksumValues(m map[time.Time]map[string]string) func(_ context.Context, i interface{}) (interface{}, error) {
	return func(_ context.Context, i interface{}) (interface{}, error) {
		v := i.(*Data)
		checksum := m[v.GeneratedAt][v.TargetType]
		if v.TargetType != "checksum" && checksum == "" {
			return nil, fmt.Errorf(
				"checksum not found for %s %s",
				v.GeneratedAt.Format("2006-01-02"),
				v.TargetType,
			)
		}
		v.Checksum = checksum
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
		if item.Error() {
			return rxgo.Thrown(item.E)
		}
		body, ok := item.V.([]byte)
		if !ok {
			return rxgo.Thrown(fmt.Errorf("dump listing response was not a byte payload"))
		}
		p := xmlparser.NewParser[Data]()
		buf := bytes.NewBuffer(body)
		return p.Parse(ctx, xmlparser.SimpleTokenOrder(io.NopCloser(buf), "Contents"))
	}
}

func UpdateData(ctx context.Context, repo Repository, maxWorkers int) (int, error) {
	if maxWorkers <= 0 {
		return -1, fmt.Errorf("max-workers must be a positive integer")
	}
	checksums := newChecksumStore()
	catalogItems, err := fetchCatalogData(ctx, "")
	if err != nil {
		return -1, err
	}
	items := make([]interface{}, 0, len(catalogItems))
	for _, item := range catalogItems {
		items = append(items, item)
	}

	items, err = rxgo.Just(items...)().
		Map(DispatchChecksumFetch(checksums), rxgo.WithPool(maxWorkers)).
		ToSlice(len(items), rxgo.WithContext(ctx))

	if err != nil {
		return -1, err
	}

	res := <-rxgo.Just(items)().
		Map(SetChecksumValues(checksums.snapshot())).
		Map(helper.SliceMapper[*Data]()).
		Filter(NotNilFilter()).
		Reduce(helper.SliceReducer[*Data]()).
		Map(BatchInsertItems(repo)).
		Observe()
	return catalogUpdateCount(res)
}

func catalogUpdateCount(res rxgo.Item) (int, error) {
	if res.E != nil {
		return 0, res.E
	}
	updated, ok := res.V.(int)
	if !ok {
		return 0, fmt.Errorf("catalog update result did not contain a count")
	}
	return updated, nil
}

func BatchInsertItems(repo Repository) func(_ context.Context, i interface{}) (interface{}, error) {
	return func(_ context.Context, i interface{}) (interface{}, error) {
		data := i.([]*Data)
		return repo.BatchInsert(data)
	}
}

// UpdateSelectedData refreshes only the catalog rows needed by this invocation. A pinned month
// requires one catalog request; latest selection requires the index and latest-year catalog. It
// fetches at most one checksum document per selected dump date.
func UpdateSelectedData(
	ctx context.Context,
	repo Repository,
	entities []string,
	dumpMonth string,
) (int, error) {
	all, err := fetchCatalogData(ctx, dumpMonth)
	if err != nil {
		return -1, err
	}
	candidates := selectRefreshCandidates(all, entities, dumpMonth)
	checksumDocuments := make(map[time.Time]map[string]string)
	for _, candidate := range candidates {
		if _, exists := checksumDocuments[candidate.GeneratedAt]; exists {
			continue
		}
		var checksumURI string
		for _, item := range all {
			if item.TargetType == "checksum" && item.GeneratedAt.Equal(candidate.GeneratedAt) {
				checksumURI = item.Uri
				break
			}
		}
		if checksumURI == "" {
			return -1, fmt.Errorf("checksum manifest not found for %s", candidate.GeneratedAt.Format("2006-01-02"))
		}
		body, fetchErr := fetchBytes(ctx, checksumDownloadURL(checksumURI))
		if fetchErr != nil {
			return -1, fetchErr
		}
		checksums := make(map[string]string)
		for _, line := range strings.Split(string(body), "\n") {
			if checksum, ok := parseChecksumTextLine(line); ok && checksum.gen.Equal(candidate.GeneratedAt) {
				checksums[checksum.typ] = checksum.chk
			}
		}
		checksumDocuments[candidate.GeneratedAt] = checksums
	}

	for _, candidate := range candidates {
		checksum := checksumDocuments[candidate.GeneratedAt][candidate.TargetType]
		if checksum == "" {
			return -1, fmt.Errorf(
				"checksum not found for %s %s",
				candidate.GeneratedAt.Format("2006-01-02"),
				candidate.TargetType,
			)
		}
		candidate.Checksum = checksum
	}
	return repo.BatchInsert(candidates)
}

func selectRefreshCandidates(items []*Data, entities []string, dumpMonth string) []*Data {
	latest := make(map[string]*Data)
	selected := make(map[string]bool)
	for _, entity := range entities {
		selected[strings.TrimSuffix(strings.ToLower(entity), "s")+"s"] = true
	}
	for _, item := range items {
		if !selected[item.TargetType] {
			continue
		}
		if dumpMonth != "" && item.GeneratedAt.Format("2006-01") != dumpMonth {
			continue
		}
		current := latest[item.TargetType]
		if current == nil || item.GeneratedAt.After(current.GeneratedAt) {
			latest[item.TargetType] = item
		}
	}
	candidates := make([]*Data, 0, len(latest))
	for _, entity := range entities {
		plural := strings.TrimSuffix(strings.ToLower(entity), "s") + "s"
		if candidate := latest[plural]; candidate != nil {
			candidates = append(candidates, candidate)
		}
	}
	return candidates
}

func fetchBytes(ctx context.Context, uri string) ([]byte, error) {
	for item := range getClient().Get(ctx, uri).Observe() {
		if item.Error() {
			return nil, item.E
		}
		body, ok := item.V.([]byte)
		if !ok {
			return nil, fmt.Errorf("unexpected response body for %s", uri)
		}
		return body, nil
	}
	return nil, fmt.Errorf("empty response from %s", uri)
}

type ImportPlan struct {
	Resources map[string]string
	Dumps     []*opendiscogsmodel.DiscogsDump
}

func FetchImportPlan(k *koanf.Koanf, dataRepo Repository) (*ImportPlan, error) {
	plan, err := ResolveImportPlan(k, dataRepo)
	if err != nil {
		return nil, err
	}
	if err := FetchImportResources(context.Background(), plan); err != nil {
		return nil, err
	}
	return plan, nil
}

func ResolveImportPlan(k *koanf.Koanf, dataRepo Repository) (*ImportPlan, error) {
	plan := &ImportPlan{
		Resources: make(map[string]string),
		Dumps:     make([]*opendiscogsmodel.DiscogsDump, 0, len(k.Strings("entities"))),
	}
	dumpMonth := k.String("dump-month")
	dataRootDir := k.String("data-dir")
	for _, entity := range k.Strings("entities") {
		var (
			d   *opendiscogsmodel.DiscogsDump
			err error
		)
		if dumpMonth == "" {
			d, err = dataRepo.FindLatestByType(entity)
		} else {
			parts := strings.SplitN(dumpMonth, "-", 2)
			d, err = dataRepo.FindByYearMonthType(parts[0], parts[1], entity)
		}
		if err != nil {
			return nil, err
		}
		var (
			targetPath  = filepath.Join(dataRootDir, helper.GetLastUriSegment(d.URI))
			resourceKey = strings.TrimSuffix(entity, "s") + "s"
		)
		plan.Resources[resourceKey] = targetPath
		plan.Dumps = append(plan.Dumps, d)
	}
	return plan, nil
}

func FetchImportResources(ctx context.Context, plan *ImportPlan) error {
	handler := file.NewHandler()
	for _, dump := range plan.Dumps {
		resourceKey := strings.TrimSuffix(dump.EntityType, "s") + "s"
		targetPath, found := plan.Resources[resourceKey]
		if !found {
			return fmt.Errorf("missing %s import resource path", dump.EntityType)
		}
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fmt.Errorf("create %s import directory: %w", dump.EntityType, err)
		}
		resourceURI := dataDownloadURL(dump.URI)
		if err := handler.FetchAndCheckContext(
			ctx,
			resourceURI,
			targetPath,
			dump.ChecksumSHA256,
		); err != nil {
			return err
		}
	}
	return nil
}

func FetchFiles(k *koanf.Koanf, dataRepo Repository) (map[string]string, error) {
	plan, err := FetchImportPlan(k, dataRepo)
	if err != nil {
		return nil, err
	}
	return plan.Resources, nil
}
