package batch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	opendiscogsmanifest "github.com/dsub-io/open-discogs-model/manifest"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
)

const (
	dependencyCheckpointQuery = `
select checkpoint.dump_date,
       checkpoint.checksum_sha256,
       expected.dump_date,
       expected.checksum_sha256
  from discogs_import_checkpoint checkpoint
  left join lateral (
        select dump.dump_date,
               dump.checksum_sha256
          from discogs_dump dump
         where dump.entity_type = $1
           and dump.dump_date < $2
         order by dump.dump_date desc, dump.id desc
         limit 1
       ) expected on true
 where checkpoint.entity_type = $1`
	dependencyDateFormat = "2006-01-02"
)

var requiredImportDependencyTypes = opendiscogsmanifest.RequiredLockEntityTypes

type dependencyRequirement struct {
	entityType       string
	requiredBy       []string
	horizonExclusive time.Time
}

type dependencyCheckpoint struct {
	dumpDate       time.Time
	checksumSHA256 string
}

func preflightImportDependencies(
	ctx context.Context,
	db *sql.DB,
	dumps []*opendiscogsmodel.DiscogsDump,
) error {
	requirements, err := importDependencyRequirements(dumps)
	if err != nil {
		return err
	}
	if len(requirements) == 0 {
		return nil
	}
	if db == nil {
		return errors.New("preflight import dependencies: database connection is nil")
	}

	for _, requirement := range requirements {
		checkpoint, expected, readErr := readDependencyCheckpoint(ctx, db, requirement)
		if readErr != nil {
			return readErr
		}
		if compatibilityErr := validateDependencyCheckpoint(
			requirement,
			checkpoint,
			expected,
		); compatibilityErr != nil {
			return compatibilityErr
		}
	}
	return nil
}

func importDependencyRequirements(
	dumps []*opendiscogsmodel.DiscogsDump,
) ([]dependencyRequirement, error) {
	selected := make(map[string]*opendiscogsmodel.DiscogsDump, len(dumps))
	for _, dump := range dumps {
		if dump == nil {
			return nil, errors.New("dependency preflight contains a nil dump")
		}
		entityTypes, err := opendiscogsmanifest.OrderedEntityTypes([]string{dump.EntityType})
		if err != nil {
			return nil, fmt.Errorf("resolve dependency preflight entity: %w", err)
		}
		entityType := entityTypes[0]
		if _, duplicate := selected[entityType]; duplicate {
			return nil, fmt.Errorf("dependency preflight contains duplicate entity type %q", entityType)
		}
		if dump.DumpDate.IsZero() {
			return nil, fmt.Errorf("dependency preflight dump date is required for %q", entityType)
		}
		selected[entityType] = dump
	}

	requirementsByType := make(map[string]dependencyRequirement)
	for entityType, dump := range selected {
		dependencyTypes, err := requiredImportDependencyTypes([]string{entityType})
		if err != nil {
			return nil, fmt.Errorf("resolve %s import dependencies: %w", entityType, err)
		}
		horizon := firstDayOfNextMonth(dump.DumpDate)
		for _, dependencyType := range dependencyTypes {
			if dependencyType == entityType {
				continue
			}
			if _, included := selected[dependencyType]; included {
				continue
			}

			requirement := requirementsByType[dependencyType]
			requirement.entityType = dependencyType
			if horizon.After(requirement.horizonExclusive) {
				requirement.horizonExclusive = horizon
			}
			requirement.requiredBy = append(requirement.requiredBy, entityType)
			requirementsByType[dependencyType] = requirement
		}
	}

	entityTypes := make([]string, 0, len(requirementsByType))
	for entityType := range requirementsByType {
		entityTypes = append(entityTypes, entityType)
	}
	sort.Strings(entityTypes)
	requirements := make([]dependencyRequirement, 0, len(entityTypes))
	for _, entityType := range entityTypes {
		requirement := requirementsByType[entityType]
		sort.Strings(requirement.requiredBy)
		requirements = append(requirements, requirement)
	}
	return requirements, nil
}

func firstDayOfNextMonth(value time.Time) time.Time {
	return time.Date(value.Year(), value.Month()+1, 1, 0, 0, 0, 0, time.UTC)
}

func readDependencyCheckpoint(
	ctx context.Context,
	db *sql.DB,
	requirement dependencyRequirement,
) (dependencyCheckpoint, *dependencyCheckpoint, error) {
	var (
		checkpoint       dependencyCheckpoint
		expectedDate     sql.NullTime
		expectedChecksum sql.NullString
	)
	err := db.QueryRowContext(
		ctx,
		dependencyCheckpointQuery,
		requirement.entityType,
		requirement.horizonExclusive,
	).Scan(
		&checkpoint.dumpDate,
		&checkpoint.checksumSHA256,
		&expectedDate,
		&expectedChecksum,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return dependencyCheckpoint{}, nil, fmt.Errorf(
			"partial import requires a successful %s checkpoint for %s",
			requirement.entityType,
			strings.Join(requirement.requiredBy, ","),
		)
	}
	if err != nil {
		return dependencyCheckpoint{}, nil, fmt.Errorf(
			"read %s dependency checkpoint: %w",
			requirement.entityType,
			err,
		)
	}
	if !expectedDate.Valid || !expectedChecksum.Valid {
		return checkpoint, nil, nil
	}
	return checkpoint, &dependencyCheckpoint{
		dumpDate:       expectedDate.Time,
		checksumSHA256: expectedChecksum.String,
	}, nil
}

func validateDependencyCheckpoint(
	requirement dependencyRequirement,
	checkpoint dependencyCheckpoint,
	expected *dependencyCheckpoint,
) error {
	if !checkpoint.dumpDate.Before(requirement.horizonExclusive) {
		return nil
	}
	if expected == nil {
		return fmt.Errorf(
			"%s checkpoint %s has no immutable catalog provenance before %s",
			requirement.entityType,
			checkpoint.dumpDate.Format(dependencyDateFormat),
			requirement.horizonExclusive.Format(dependencyDateFormat),
		)
	}

	checkpointDate := checkpoint.dumpDate.Format(dependencyDateFormat)
	expectedDate := expected.dumpDate.Format(dependencyDateFormat)
	staleDate := checkpointDate < expectedDate
	reissued := checkpointDate == expectedDate && !strings.EqualFold(
		strings.TrimSpace(checkpoint.checksumSHA256),
		strings.TrimSpace(expected.checksumSHA256),
	)
	if !staleDate && !reissued {
		return nil
	}
	return fmt.Errorf(
		"%s checkpoint %s is stale for %s; latest compatible catalog dump is %s",
		requirement.entityType,
		checkpointDate,
		strings.Join(requirement.requiredBy, ","),
		expectedDate,
	)
}
