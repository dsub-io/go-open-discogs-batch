package cmd

import (
	"errors"
	"fmt"
	"github.com/knadh/koanf"
	"regexp"
	"strconv"
	"strings"
)

var YearPattern = regexp.MustCompile(`^\d{4}$`)
var MonthPattern = regexp.MustCompile(`^(0?[1-9]|1[0-2])$`)
var PluralPattern = regexp.MustCompile(`^.*s$`)
var DslPattern = regexp.MustCompile(`^postgres(?:ql)?://([^/]+:[^/]+)@([^/]+:\d+)(/.*)?$`)

type ConfigValidator interface {
	Validate(k koanf.Koanf) error
}

type validator struct{}

// Validate command values
func (v *validator) Validate(koanf *koanf.Koanf) error {
	y, m := koanf.String("year"), koanf.String("month")
	if err := ValidYearMonth(y, m); err != nil {
		return err
	} else if err = ValidTypes(koanf.Strings("types")); err != nil {
		return err
	} else if err = ValidDsnFormat(koanf.String("dsn")); err != nil {
		return err
	}
	return ValidChunkSize(koanf.String("chunk"))
}

func ValidChunkSize(chunkSizeVal string) (err error) {
	if ok, _ := regexp.MatchString("^\\d+$", chunkSizeVal); !ok {
		err = fmt.Errorf("invalid chunk option: %+v", chunkSizeVal)
	} else if n, _ := strconv.Atoi(chunkSizeVal); n <= 0 {
		err = fmt.Errorf("chunk size cannot be zero or negative")
	}
	return
}

func ValidDsnFormat(dsn string) (err error) {
	if len(dsn) == 0 {
		err = fmt.Errorf("missing dsn")
	} else if !DslPattern.MatchString(dsn) {
		err = fmt.Errorf("failed to verify dsn: invalid format")
	}
	return
}

func ValidTypes(types []string) (err error) {
	u := make([]string, 0)
	for _, t := range types {
		s := t
		if !PluralPattern.MatchString(t) {
			s += "s"
		}
		if !containsKey(Types, s) {
			u = append(u, t)
		}
	}
	if len(u) > 0 {
		err = fmt.Errorf("unknown types: [%+v]", strings.Join(u, ","))
	}
	return
}

// ValidYearMonth validates year and month if it has given user command value.
func ValidYearMonth(y, m string) (err error) {
	if len(y) > 0 && !YearPattern.MatchString(y) { // invalid year set
		return errors.New("invalid year")
	}

	if len(m) > 0 && !MonthPattern.MatchString(m) { // invalid month set
		return fmt.Errorf("%w\ninvalid month: %+v", err, m)
	}
	return err
}
