package batch

import (
	"fmt"

	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"gorm.io/gorm"
)

type ChunkMetadata struct {
	Index          int64
	FirstItemIndex int64
	ItemCount      int
}

type completedChunkInventory map[int64]ChunkMetadata

func loadCompletedEntityProgress(order Order) (bool, int64, int64, error) {
	if !order.shouldResumeProgress() {
		return false, 0, 0, nil
	}
	type completedEntityRow struct {
		TotalItems  int64
		TotalChunks int64
	}
	var completed completedEntityRow
	query := order.getDB().WithContext(order.getContext()).Raw(
		`select total_items, total_chunks
		   from discogs_import_run_dump
		  where import_run_id = ?
		    and entity_type = ?
		    and chunk_size = ?
		    and completed_at is not null
		    and total_items is not null
		    and total_chunks is not null
		    and processed_items = total_items`,
		order.getRunID(),
		order.getEntityType(),
		order.getChunkSize(),
	).Scan(&completed)
	if query.Error != nil {
		return false, 0, 0, fmt.Errorf(
			"load %s completed progress: %w",
			order.getEntityType(),
			query.Error,
		)
	}
	if query.RowsAffected == 0 {
		return false, 0, 0, nil
	}
	return true, completed.TotalItems, completed.TotalChunks, nil
}

func completedEntityResult(order Order, topic string) (result.Result, bool) {
	completed, totalItems, _, err := loadCompletedEntityProgress(order)
	if err != nil {
		return result.NewResult(0, err), true
	}
	if !completed {
		return result.NewResult(0, nil), false
	}
	fmt.Printf("Updated 0 %s (%d items already complete)\n", topic, totalItems)
	return result.NewResult(0, nil), true
}

func loadCompletedChunkInventory(order Order) (completedChunkInventory, error) {
	completed := make(completedChunkInventory)
	if !order.shouldResumeProgress() {
		return completed, nil
	}
	type completedChunkRow struct {
		ChunkIndex     int64
		FirstItemIndex int64
		ItemCount      int
	}
	var chunks []completedChunkRow
	query := order.getDB().WithContext(order.getContext()).Raw(
		`select chunk_index, first_item_index, item_count
		   from discogs_import_run_chunk
		  where import_run_id = ?
		    and entity_type = ?
		  order by chunk_index`,
		order.getRunID(),
		order.getEntityType(),
	).Scan(&chunks)
	if query.Error != nil {
		return nil, fmt.Errorf(
			"load %s completed chunks: %w",
			order.getEntityType(),
			query.Error,
		)
	}
	for _, chunk := range chunks {
		completed[chunk.ChunkIndex] = ChunkMetadata{
			Index:          chunk.ChunkIndex,
			FirstItemIndex: chunk.FirstItemIndex,
			ItemCount:      chunk.ItemCount,
		}
	}
	return completed, nil
}

func (completed completedChunkInventory) contains(chunk ChunkMetadata) (bool, error) {
	recorded, exists := completed[chunk.Index]
	if !exists {
		return false, nil
	}
	if recorded.FirstItemIndex != chunk.FirstItemIndex || recorded.ItemCount != chunk.ItemCount {
		return false, fmt.Errorf(
			"check completed chunk %d: recorded range (%d, %d) does not match source range (%d, %d)",
			chunk.Index,
			recorded.FirstItemIndex,
			recorded.ItemCount,
			chunk.FirstItemIndex,
			chunk.ItemCount,
		)
	}
	return true, nil
}

func executeActiveRunTransaction(
	order Order,
	write func(Order) result.Result,
) result.Result {
	if order.getRunID() == 0 {
		return write(order)
	}
	written := result.NewResult(0, nil)
	err := order.getDB().WithContext(order.getContext()).Transaction(func(tx *gorm.DB) error {
		written = write(order.withDB(tx))
		if written.IsErr() {
			return written.Err()
		}
		type activeImportRun struct {
			ID int64
		}
		var active activeImportRun
		fenced := tx.Raw(
			`select id
			   from discogs_import_run
			  where id = ?
			    and status = 'running'
			  for update`,
			order.getRunID(),
		).Scan(&active)
		if fenced.Error != nil {
			return fmt.Errorf("fence active import run %d: %w", order.getRunID(), fenced.Error)
		}
		if fenced.RowsAffected != 1 || active.ID != order.getRunID() {
			return fmt.Errorf("fence active import run %d: run is not active", order.getRunID())
		}
		return nil
	})
	if err != nil {
		return result.NewResult(0, err)
	}
	return written
}

func executeChunk(
	order Order,
	chunk ChunkMetadata,
	write func(Order) result.Result,
) result.Result {
	db := order.getDB().WithContext(order.getContext())
	written := result.NewResult(0, nil)
	err := db.Transaction(func(tx *gorm.DB) error {
		if order.shouldResumeProgress() {
			completed, checkErr := chunkAlreadyCompleted(tx, order, chunk)
			if checkErr != nil {
				return checkErr
			}
			if completed {
				return nil
			}
		}

		written = write(order.withDB(tx))
		if written.IsErr() {
			return written.Err()
		}
		if order.getRunID() == 0 {
			return nil
		}
		return recordCompletedChunk(tx, order, chunk)
	})
	if err != nil {
		return result.NewResult(0, err)
	}
	return written
}

func chunkAlreadyCompleted(
	tx *gorm.DB,
	order Order,
	chunk ChunkMetadata,
) (bool, error) {
	type recordedChunk struct {
		FirstItemIndex int64
		ItemCount      int
	}
	var recorded recordedChunk
	query := tx.Raw(
		`select first_item_index, item_count
		   from discogs_import_run_chunk
		  where import_run_id = ?
		    and entity_type = ?
		    and chunk_index = ?`,
		order.getRunID(),
		order.getEntityType(),
		chunk.Index,
	).Scan(&recorded)
	if query.Error != nil {
		return false, fmt.Errorf(
			"check %s chunk %d progress: %w",
			order.getEntityType(),
			chunk.Index,
			query.Error,
		)
	}
	if query.RowsAffected == 0 {
		return false, nil
	}
	if recorded.FirstItemIndex != chunk.FirstItemIndex || recorded.ItemCount != chunk.ItemCount {
		return false, fmt.Errorf(
			"check %s chunk %d progress: recorded range (%d, %d) does not match source range (%d, %d)",
			order.getEntityType(),
			chunk.Index,
			recorded.FirstItemIndex,
			recorded.ItemCount,
			chunk.FirstItemIndex,
			chunk.ItemCount,
		)
	}
	return true, nil
}

func recordCompletedChunk(
	tx *gorm.DB,
	order Order,
	chunk ChunkMetadata,
) error {
	updated := tx.Exec(
		`with active_run as (
		    select id
		      from discogs_import_run
		     where id = ?
		       and status = 'running'
		     for update
		),
		inserted as (
		    insert into discogs_import_run_chunk
		        (import_run_id, entity_type, chunk_index, first_item_index, item_count)
		    select active_run.id, ?, ?, ?, ?
		      from active_run
		    on conflict do nothing
		    returning item_count
		)
		update discogs_import_run_dump run_dump
		   set processed_items = run_dump.processed_items + inserted.item_count,
		       last_progress_at = now()
		  from inserted
		 where run_dump.import_run_id = ?
		   and run_dump.entity_type = ?
		   and run_dump.chunk_size = ?
		   and run_dump.completed_at is null`,
		order.getRunID(),
		order.getEntityType(),
		chunk.Index,
		chunk.FirstItemIndex,
		chunk.ItemCount,
		order.getRunID(),
		order.getEntityType(),
		order.getChunkSize(),
	)
	if updated.Error != nil {
		return fmt.Errorf(
			"record %s chunk %d progress: %w",
			order.getEntityType(),
			chunk.Index,
			updated.Error,
		)
	}
	if updated.RowsAffected != 1 {
		return fmt.Errorf(
			"record %s chunk %d progress: run is not active, completion already exists, or run summary is unavailable",
			order.getEntityType(),
			chunk.Index,
		)
	}
	return nil
}

func completeEntityProgress(order Order, totalItems, totalChunks int64) error {
	if order.getRunID() == 0 {
		return nil
	}
	updated := order.getDB().WithContext(order.getContext()).Exec(
		`with active_run as (
		    select id
		      from discogs_import_run
		     where id = ?
		       and status = 'running'
		     for update
		),
		coverage as (
		    select count(*) as completed_chunks,
		           coalesce(sum(item_count), 0) as completed_items,
		           count(*) filter (
		               where chunk_index >= ?
		                  or first_item_index <> chunk_index * ?
		                  or item_count <> case
		                      when chunk_index = ? - 1 then ? - first_item_index
		                      else ?
		                  end
		           ) as invalid_chunks
		      from discogs_import_run_chunk
		     where import_run_id = ?
		       and entity_type = ?
		)
		update discogs_import_run_dump
		   set total_items = ?,
		       total_chunks = ?,
		       completed_at = now(),
		       last_progress_at = now()
		  from coverage, active_run
		 where import_run_id = active_run.id
		   and entity_type = ?
		   and chunk_size = ?
		   and processed_items = ?
		   and coverage.completed_chunks = ?
		   and coverage.completed_items = ?
		   and coverage.invalid_chunks = 0`,
		order.getRunID(),
		totalChunks,
		order.getChunkSize(),
		totalChunks,
		totalItems,
		order.getChunkSize(),
		order.getRunID(),
		order.getEntityType(),
		totalItems,
		totalChunks,
		order.getEntityType(),
		order.getChunkSize(),
		totalItems,
		totalChunks,
		totalItems,
	)
	if updated.Error != nil {
		return fmt.Errorf("complete %s progress: %w", order.getEntityType(), updated.Error)
	}
	if updated.RowsAffected != 1 {
		return fmt.Errorf(
			"complete %s progress: chunk coverage does not match %d items in %d chunks",
			order.getEntityType(),
			totalItems,
			totalChunks,
		)
	}
	return nil
}
