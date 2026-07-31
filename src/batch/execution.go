package batch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	opendiscogsmanifest "github.com/dsub-io/open-discogs-model/manifest"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
)

const processorName = "go-open-discogs-batch"

type ImportPreparation struct {
	ManifestSHA256 string
	RunID          int64
	Skipped        bool
}

type ImportExecutionCoordinator struct {
	db               *sql.DB
	processorVersion string

	mu       sync.Mutex
	conn     *sql.Conn
	runID    int64
	lockKeys []int32
}

func NewImportExecutionCoordinator(
	db *sql.DB,
	processorVersion string,
) *ImportExecutionCoordinator {
	if strings.TrimSpace(processorVersion) == "" {
		processorVersion = "development"
	}
	return &ImportExecutionCoordinator{
		db:               db,
		processorVersion: processorVersion,
	}
}

func (c *ImportExecutionCoordinator) Prepare(
	ctx context.Context,
	dumps []*opendiscogsmodel.DiscogsDump,
	force bool,
	allowDowngrade bool,
) (*ImportPreparation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil, errors.New("import execution has already been prepared")
	}
	if len(dumps) == 0 {
		return nil, errors.New("import plan must contain at least one dump")
	}

	manifestDumps := make([]opendiscogsmanifest.Dump, 0, len(dumps))
	entityTypes := make([]string, 0, len(dumps))
	for _, dump := range dumps {
		if dump == nil {
			return nil, errors.New("import plan contains a nil dump")
		}
		manifestDumps = append(manifestDumps, opendiscogsmanifest.Dump{
			EntityType:     dump.EntityType,
			DumpDate:       dump.DumpDate,
			ChecksumSHA256: dump.ChecksumSHA256,
		})
		entityTypes = append(entityTypes, dump.EntityType)
	}

	fingerprint, err := opendiscogsmanifest.Fingerprint(manifestDumps)
	if err != nil {
		return nil, fmt.Errorf("fingerprint import manifest: %w", err)
	}
	orderedTypes, err := opendiscogsmanifest.OrderedEntityTypes(entityTypes)
	if err != nil {
		return nil, err
	}

	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("reserve import lock connection: %w", err)
	}
	c.conn = conn
	if err := c.acquireEntityLocks(ctx, orderedTypes); err != nil {
		c.release(ctx)
		return nil, err
	}

	tx, err := c.conn.BeginTx(ctx, nil)
	if err != nil {
		c.release(ctx)
		return nil, fmt.Errorf("begin import admission transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := markAbandonedRuns(ctx, tx, orderedTypes); err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, err
	}
	if err := assertNotDowngrade(ctx, tx, dumps, allowDowngrade); err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, err
	}

	successfulRunID, err := findSuccessfulRun(ctx, tx, fingerprint)
	if err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, err
	}
	if successfulRunID != 0 && !force {
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, fmt.Errorf("commit skipped import admission: %w", err)
		}
		committed = true
		c.release(ctx)
		return &ImportPreparation{
			ManifestSHA256: fingerprint,
			RunID:          successfulRunID,
			Skipped:        true,
		}, nil
	}

	dumpIDs := make([]int64, len(dumps))
	for index, dump := range dumps {
		dumpIDs[index], err = findOrInsertDump(ctx, tx, dump)
		if err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, err
		}
	}

	runID, err := insertImportRun(
		ctx,
		tx,
		fingerprint,
		force,
		allowDowngrade,
		c.processorVersion,
	)
	if err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, err
	}
	for index, dump := range dumps {
		if _, err := tx.ExecContext(
			ctx,
			`insert into public.discogs_import_run_dump
			    (import_run_id, entity_type, dump_id)
			 values ($1, $2, $3)`,
			runID,
			dump.EntityType,
			dumpIDs[index],
		); err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, fmt.Errorf("record import run dump %s: %w", dump.EntityType, err)
		}
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, fmt.Errorf("commit import admission: %w", err)
	}
	committed = true
	c.runID = runID
	return &ImportPreparation{
		ManifestSHA256: fingerprint,
		RunID:          runID,
	}, nil
}

func (c *ImportExecutionCoordinator) Complete(ctx context.Context, runErr error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil || c.runID == 0 {
		return nil
	}
	defer c.release(ctx)

	tx, err := c.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin import completion transaction: %w", err)
	}
	status := "success"
	var failure any
	if runErr != nil {
		status = "failed"
		failure = runErr.Error()
	}
	result, err := tx.ExecContext(
		ctx,
		`update public.discogs_import_run
		    set status = $1,
		        completed_at = now(),
		        failure_message = $2
		  where id = $3
		    and status = 'running'`,
		status,
		failure,
		c.runID,
	)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("complete import run %d: %w", c.runID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("read import completion result: %w", err)
	}
	if affected != 1 {
		_ = tx.Rollback()
		return fmt.Errorf("import run %d was not running", c.runID)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit import completion: %w", err)
	}
	c.runID = 0
	return nil
}

func (c *ImportExecutionCoordinator) acquireEntityLocks(
	ctx context.Context,
	entityTypes []string,
) error {
	for _, entityType := range entityTypes {
		key, err := opendiscogsmanifest.EntityLockKey(entityType)
		if err != nil {
			return err
		}
		var acquired bool
		if err := c.conn.QueryRowContext(
			ctx,
			"select pg_try_advisory_lock($1, $2)",
			opendiscogsmanifest.AdvisoryLockNamespace,
			key,
		).Scan(&acquired); err != nil {
			return fmt.Errorf("acquire %s import lock: %w", entityType, err)
		}
		if !acquired {
			return fmt.Errorf("another import is already updating %s", entityType)
		}
		c.lockKeys = append(c.lockKeys, key)
	}
	return nil
}

func (c *ImportExecutionCoordinator) release(ctx context.Context) {
	if c.conn == nil {
		return
	}
	for index := len(c.lockKeys) - 1; index >= 0; index-- {
		_, _ = c.conn.ExecContext(
			ctx,
			"select pg_advisory_unlock($1, $2)",
			opendiscogsmanifest.AdvisoryLockNamespace,
			c.lockKeys[index],
		)
	}
	c.lockKeys = nil
	_ = c.conn.Close()
	c.conn = nil
	c.runID = 0
}

func markAbandonedRuns(
	ctx context.Context,
	tx *sql.Tx,
	entityTypes []string,
) error {
	placeholders := make([]string, len(entityTypes))
	args := make([]any, len(entityTypes))
	for index, entityType := range entityTypes {
		placeholders[index] = fmt.Sprintf("$%d", index+1)
		args[index] = entityType
	}
	query := fmt.Sprintf(`
		update public.discogs_import_run import_run
		   set status = 'failed',
		       completed_at = now(),
		       failure_message = 'recovered after entity advisory locks were released'
		 where import_run.status = 'running'
		   and exists (
		       select 1
		         from public.discogs_import_run_dump run_dump
		        where run_dump.import_run_id = import_run.id
		          and run_dump.entity_type in (%s)
		   )`, strings.Join(placeholders, ", "))
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("recover abandoned import runs: %w", err)
	}
	return nil
}

func assertNotDowngrade(
	ctx context.Context,
	tx *sql.Tx,
	dumps []*opendiscogsmodel.DiscogsDump,
	allowDowngrade bool,
) error {
	if allowDowngrade {
		return nil
	}
	for _, dump := range dumps {
		var checkpointDate sql.NullTime
		err := tx.QueryRowContext(
			ctx,
			`select dump_date
			   from public.discogs_import_checkpoint
			  where entity_type = $1`,
			dump.EntityType,
		).Scan(&checkpointDate)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read %s import checkpoint: %w", dump.EntityType, err)
		}
		if checkpointDate.Valid &&
			opendiscogsmanifest.IsDowngrade(dump.DumpDate, checkpointDate.Time) {
			return fmt.Errorf(
				"dump %s %s predates checkpoint %s; use --allow-downgrade to override",
				dump.EntityType,
				dump.DumpDate.Format("2006-01-02"),
				checkpointDate.Time.Format("2006-01-02"),
			)
		}
	}
	return nil
}

func findSuccessfulRun(
	ctx context.Context,
	tx *sql.Tx,
	fingerprint string,
) (int64, error) {
	var runID int64
	err := tx.QueryRowContext(
		ctx,
		`select id
		   from public.discogs_import_run
		  where manifest_sha256 = $1
		    and status = 'success'
		  order by completed_at desc, id desc
		  limit 1`,
		fingerprint,
	).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("find successful import manifest: %w", err)
	}
	return runID, nil
}

func findOrInsertDump(
	ctx context.Context,
	tx *sql.Tx,
	dump *opendiscogsmodel.DiscogsDump,
) (int64, error) {
	var dumpID int64
	err := tx.QueryRowContext(
		ctx,
		`insert into public.discogs_dump
		    (etag, dump_date, entity_type, checksum_sha256, size_bytes, uri)
		 values ($1, $2, $3, $4, $5, $6)
		 on conflict (dump_date, entity_type, checksum_sha256) do nothing
		 returning id`,
		dump.ETag,
		dump.DumpDate,
		dump.EntityType,
		dump.ChecksumSHA256,
		dump.SizeBytes,
		dump.URI,
	).Scan(&dumpID)
	if err == nil {
		return dumpID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("record %s dump provenance: %w", dump.EntityType, err)
	}
	err = tx.QueryRowContext(
		ctx,
		`select id
		   from public.discogs_dump
		  where dump_date = $1
		    and entity_type = $2
		    and checksum_sha256 = $3`,
		dump.DumpDate,
		dump.EntityType,
		dump.ChecksumSHA256,
	).Scan(&dumpID)
	if err != nil {
		return 0, fmt.Errorf("resolve %s dump provenance: %w", dump.EntityType, err)
	}
	return dumpID, nil
}

func insertImportRun(
	ctx context.Context,
	tx *sql.Tx,
	fingerprint string,
	force bool,
	allowDowngrade bool,
	processorVersion string,
) (int64, error) {
	var runID int64
	err := tx.QueryRowContext(
		ctx,
		`insert into public.discogs_import_run
		    (manifest_sha256, status, force_requested,
		     allow_downgrade_requested, processor, processor_version)
		 values ($1, 'running', $2, $3, $4, $5)
		 returning id`,
		fingerprint,
		force,
		allowDowngrade,
		processorName,
		processorVersion,
	).Scan(&runID)
	if err != nil {
		return 0, fmt.Errorf("start import run: %w", err)
	}
	return runID, nil
}
