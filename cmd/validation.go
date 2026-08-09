package cmd

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/knadh/koanf"
)

var dumpMonthPattern = regexp.MustCompile(`^\d{4}-\d{2}$`)

type ConfigValidator interface {
	Validate(k koanf.Koanf) error
}

type validator struct{}

func (v *validator) Validate(config *koanf.Koanf) error {
	for _, key := range []string{"cleanup", "force", "allow-downgrade"} {
		if _, invalid := config.Get(key).(string); invalid {
			return fmt.Errorf("OPEN_DISCOGS_BATCH_%s must be a boolean value", strings.ToUpper(strings.ReplaceAll(key, "-", "_")))
		}
	}
	if err := ValidDumpMonth(config.String("dump-month")); err != nil {
		return err
	}
	if err := ValidEntities(config.Strings("entities")); err != nil {
		return err
	}
	if err := ValidDatabaseURL(config.String("database-url")); err != nil {
		return err
	}
	if err := ValidChunkSize(config.String("chunk-size")); err != nil {
		return err
	}
	return ValidMaxWorkers(config.String("max-workers"))
}

func ValidMaxWorkers(value string) error {
	return validPositiveInteger("max-workers", value)
}

func ValidChunkSize(value string) error {
	return validPositiveInteger("chunk-size", value)
}

func validPositiveInteger(name, value string) error {
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed <= 0 {
		return fmt.Errorf("%s must be a positive integer", name)
	}
	return nil
}

func ValidDatabaseURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		return fmt.Errorf("database-url must use postgresql://user:password@host:port/database format")
	}
	if parsed.User == nil || parsed.User.Username() == "" {
		return fmt.Errorf("database-url must include a username and password")
	}
	if _, present := parsed.User.Password(); !present {
		return fmt.Errorf("database-url must include a username and password")
	}
	if parsed.Hostname() == "" || strings.Trim(parsed.Path, "/") == "" {
		return fmt.Errorf("database-url must include a host and database name")
	}
	return nil
}

func ValidEntities(entities []string) error {
	known := map[string]bool{"artist": true, "label": true, "master": true, "release": true}
	unknown := make([]string, 0)
	for _, entity := range entities {
		if !known[strings.TrimSuffix(strings.ToLower(entity), "s")] {
			unknown = append(unknown, entity)
		}
	}
	if len(entities) == 0 {
		return fmt.Errorf("entities must not be empty")
	}
	if len(unknown) > 0 {
		return fmt.Errorf("unknown entities: [%s]", strings.Join(unknown, ","))
	}
	return nil
}

func ValidDumpMonth(value string) error {
	if value == "" {
		return nil
	}
	if !dumpMonthPattern.MatchString(value) {
		return fmt.Errorf("dump-month must use yyyy-MM format")
	}
	target, err := time.Parse("2006-01", value)
	if err != nil {
		return fmt.Errorf("dump-month must use yyyy-MM format")
	}
	earliest, current := time.Date(2008, 3, 1, 0, 0, 0, 0, time.UTC), time.Now().UTC()
	current = time.Date(current.Year(), current.Month(), 1, 0, 0, 0, 0, time.UTC)
	if target.Before(earliest) || target.After(current) {
		return fmt.Errorf("dump-month must be between 2008-03 and %s", current.Format("2006-01"))
	}
	return nil
}
