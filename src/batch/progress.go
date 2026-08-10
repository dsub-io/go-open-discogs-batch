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

func executeChunk(
	order Order,
	chunk ChunkMetadata,
	write func(Order) result.Result,
) result.Result {
	db := order.getDB().WithContext(order.getContext())
	written := result.NewResult(0, nil)
	err := db.Transaction(func(tx *gorm.DB) error {
		if order.getRunID() != 0 {
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
		   from public.discogs_import_run_chunk
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
	inserted := tx.Exec(
		`insert into public.discogs_import_run_chunk
		    (import_run_id, entity_type, chunk_index, first_item_index, item_count)
		 values (?, ?, ?, ?, ?)
		 on conflict do nothing`,
		order.getRunID(),
		order.getEntityType(),
		chunk.Index,
		chunk.FirstItemIndex,
		chunk.ItemCount,
	)
	if inserted.Error != nil {
		return fmt.Errorf(
			"record %s chunk %d progress: %w",
			order.getEntityType(),
			chunk.Index,
			inserted.Error,
		)
	}
	if inserted.RowsAffected != 1 {
		return fmt.Errorf(
			"record %s chunk %d progress: completion already exists",
			order.getEntityType(),
			chunk.Index,
		)
	}

	updated := tx.Exec(
		`update public.discogs_import_run_dump
		    set processed_items = processed_items + ?,
		        last_progress_at = now()
		  where import_run_id = ?
		    and entity_type = ?
		    and chunk_size = ?
		    and completed_at is null`,
		chunk.ItemCount,
		order.getRunID(),
		order.getEntityType(),
		order.getChunkSize(),
	)
	if updated.Error != nil {
		return fmt.Errorf(
			"advance %s chunk %d progress: %w",
			order.getEntityType(),
			chunk.Index,
			updated.Error,
		)
	}
	if updated.RowsAffected != 1 {
		return fmt.Errorf(
			"advance %s chunk %d progress: run summary is unavailable",
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
		`with coverage as (
		    select count(*) as completed_chunks,
		           coalesce(sum(item_count), 0) as completed_items,
		           count(*) filter (
		               where first_item_index <> chunk_index * ?
		                  or item_count <> case
		                      when chunk_index = ? - 1 then ? - first_item_index
		                      else ?
		                  end
		           ) as invalid_chunks
		      from public.discogs_import_run_chunk
		     where import_run_id = ?
		       and entity_type = ?
		)
		update public.discogs_import_run_dump
		   set total_items = ?,
		       total_chunks = ?,
		       completed_at = now(),
		       last_progress_at = now()
		  from coverage
		 where import_run_id = ?
		   and entity_type = ?
		   and chunk_size = ?
		   and processed_items = ?
		   and coverage.completed_chunks = ?
		   and coverage.completed_items = ?
		   and coverage.invalid_chunks = 0`,
		order.getChunkSize(),
		totalChunks,
		totalItems,
		order.getChunkSize(),
		order.getRunID(),
		order.getEntityType(),
		totalItems,
		totalChunks,
		order.getRunID(),
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
