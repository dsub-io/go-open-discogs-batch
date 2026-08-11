package data

import (
	"context"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	catalogPrefixParameter = "prefix"
	downloadParameter      = "download"
	dumpPathPrefix         = "data/"
)

var (
	catalogAnchorPattern = regexp.MustCompile(`<a\s+href="([^"]+)"`)
	catalogYearPattern   = regexp.MustCompile(`^data/(\d{4})/$`)
	yearPattern          = regexp.MustCompile(`^\d{4}$`)
)

func fetchCatalogData(ctx context.Context, dumpMonth string) ([]*Data, error) {
	year, err := resolveCatalogYear(ctx, dumpMonth)
	if err != nil {
		return nil, err
	}
	body, err := fetchBytes(ctx, catalogListingURL(year))
	if err != nil {
		return nil, fmt.Errorf("fetch %s dump catalog: %w", year, err)
	}
	items := parseCatalogData(body)
	if len(items) == 0 {
		return nil, fmt.Errorf("%s dump catalog contains no supported files", year)
	}
	return items, nil
}

func resolveCatalogYear(ctx context.Context, dumpMonth string) (string, error) {
	if dumpMonth != "" {
		year, _, found := strings.Cut(dumpMonth, "-")
		if !found || !yearPattern.MatchString(year) {
			return "", fmt.Errorf("invalid dump month %q", dumpMonth)
		}
		return year, nil
	}
	body, err := fetchBytes(ctx, DiscogsDataBaseURL)
	if err != nil {
		return "", fmt.Errorf("fetch dump catalog index: %w", err)
	}
	return latestCatalogYear(body)
}

func latestCatalogYear(body []byte) (string, error) {
	latest := ""
	for _, href := range catalogHrefs(body) {
		prefix := catalogQueryValue(href, catalogPrefixParameter)
		match := catalogYearPattern.FindStringSubmatch(prefix)
		if match != nil && match[1] > latest {
			latest = match[1]
		}
	}
	if latest == "" {
		return "", fmt.Errorf("dump catalog index contains no years")
	}
	return latest, nil
}

func parseCatalogData(body []byte) []*Data {
	items := make([]*Data, 0)
	for _, href := range catalogHrefs(body) {
		uri := catalogQueryValue(href, downloadParameter)
		match := dumpUriPattern.FindStringSubmatch(uri)
		if match == nil || !isKnownType(strings.ToLower(match[3])) {
			continue
		}
		generatedAt, err := parseDumpDate(match[2])
		if err != nil {
			continue
		}
		items = append(items, &Data{
			GeneratedAt: generatedAt,
			TargetType:  strings.ToLower(match[3]),
			Uri:         uri,
		})
	}
	return items
}

func parseDumpDate(value string) (time.Time, error) {
	return time.Parse(dumpDateLayout, value)
}

func catalogHrefs(body []byte) []string {
	matches := catalogAnchorPattern.FindAllSubmatch(body, -1)
	hrefs := make([]string, 0, len(matches))
	for _, match := range matches {
		hrefs = append(hrefs, html.UnescapeString(string(match[1])))
	}
	return hrefs
}

func catalogQueryValue(href string, parameter string) string {
	parsed, err := url.Parse(href)
	if err != nil {
		return ""
	}
	return parsed.Query().Get(parameter)
}

func catalogListingURL(year string) string {
	prefix := dumpPathPrefix + year + "/"
	return DiscogsDataBaseURL + "?" + catalogPrefixParameter + "=" + url.QueryEscape(prefix)
}

func dataDownloadURL(uri string) string {
	return DiscogsDataBaseURL + "?" + downloadParameter + "=" + url.QueryEscape(uri)
}
