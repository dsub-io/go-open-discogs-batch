package unique

import (
	"reflect"

	"github.com/mitchellh/hashstructure/v2"
)

func Slice[T any](items []T) []T {
	return sliceWithHasher(items, func(item T) (uint64, error) {
		return hashstructure.Hash(item, hashstructure.FormatV2, nil)
	})
}

func sliceWithHasher[T any](items []T, hashItem func(T) (uint64, error)) []T {
	buckets := make(map[uint64][]T)
	result := make([]T, 0, len(items))
	for _, item := range items {
		hash, err := hashItem(item)
		if err != nil {
			panic(err)
		}
		duplicate := false
		for _, previous := range buckets[hash] {
			if reflect.DeepEqual(previous, item) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		buckets[hash] = append(buckets[hash], item)
		result = append(result, item)
	}
	return result
}
