package dateparser

import (
	"regexp"
	"time"
)

var releaseDateDelimRegexp = regexp.MustCompile(`[-_. \D]`)
var dateByDelimRegexp = regexp.MustCompile(`^(\d+)[-_. ](\d+)[-_. ](\d+)$`)
var fourDigitYearMonthOnlyRegexp = regexp.MustCompile(`^(\d{4})(\d{1,2})\D{0,2}$`)

func ParseYMD(ymd string) (y, m, d *int16) {
	if m := dateByDelimRegexp.FindStringSubmatch(ymd); m != nil {
		month := minTwoDigit(m[2])
		day := minTwoDigit(m[3])
		ymd = m[1]
		if month != "00" {
			ymd += month
		}
		if month != "00" && day != "00" {
			ymd += day
		}
	}
	ymd = releaseDateDelimRegexp.ReplaceAllString(ymd, "")
	if m := fourDigitYearMonthOnlyRegexp.FindStringSubmatch(ymd); m != nil {
		ymd = m[1] + minTwoDigit(m[2])
	}

	if t, err := time.Parse("20060102", ymd); err == nil {
		y1, m1, d1 := int16(t.Year()), int16(t.Month()), int16(t.Day())
		return &y1, &m1, &d1
	}

	if t, err := time.Parse("200601", ymd); err == nil {
		y1, m1 := int16(t.Year()), int16(t.Month())
		return &y1, &m1, nil
	}

	if t, err := time.Parse("060102", ymd); err == nil {
		y1, m1, d1 := int16(t.Year()), int16(t.Month()), int16(t.Day())
		return &y1, &m1, &d1
	}

	if t, err := time.Parse("2006", ymd); err == nil {
		y1 := int16(t.Year())
		return &y1, nil, nil
	}

	if t, err := time.Parse("06", ymd); err == nil {
		y1 := int16(t.Year())
		return &y1, nil, nil
	}
	return nil, nil, nil
}

func minTwoDigit(s string) string {
	if len(s) == 1 {
		return "0" + s
	}
	return s
}
