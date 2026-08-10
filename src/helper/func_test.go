package helper

import (
	"github.com/reactivex/rxgo/v2"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSliceMapper(t *testing.T) {
	t.Run("slice mapper must return items into slice", func(t *testing.T) {
		s := "test string"
		mapper := SliceMapper[string]()
		v, err := mapper(nil, s)
		require.NoError(t, err)
		var forTyp []string
		require.IsType(t, forTyp, v)
		res := v.([]string)
		require.Len(t, res, 1)
		require.Contains(t, res, s)
	})
}

func TestSliceReducer(t *testing.T) {
	t.Run("slice reducer must return first item", func(t *testing.T) {
		reducer := SliceReducer[string]()
		i, err := reducer(nil, nil, []string{"test"})
		require.NoError(t, err)
		require.IsType(t, []string{}, i)
		res := i.([]string)
		require.Contains(t, res, "test")
		require.Len(t, res, 1)
	})

	t.Run("slice reducer must return all items", func(t *testing.T) {
		reducer := SliceReducer[int]()
		i, err := reducer(nil, []int{1, 2, 3}, []int{4})
		require.NoError(t, err)
		require.IsType(t, []int{}, i)
		res := i.([]int)
		require.Len(t, res, 4)
		for _, j := range []int{1, 2, 3, 4} {
			require.Contains(t, res, j)
		}
	})
}

func TestMergeCount(t *testing.T) {
	f := MergeCount()
	t.Run("merge sums two numbers", func(t *testing.T) {
		n, err := f(nil, 1, 2)
		require.NoError(t, err)
		require.Equal(t, 3, n)
	})
	t.Run("merge sums a nil with number", func(t *testing.T) {
		n, err := f(nil, nil, 2)
		require.NoError(t, err)
		require.Equal(t, 2, n)
	})
}

func TestMapWindowedSlice(t *testing.T) {
	f := MapWindowedSlice[int]()

	t.Run("mapper ", func(t *testing.T) {
		for result := range rxgo.Range(1, 100).
			WindowWithCount(3).
			Map(f).
			Observe() {
			require.NotNil(t, result)
			require.IsType(t, make([]int, 0), result.V)
			values := result.V.([]int)
			require.NotEmpty(t, values)
		}
	})

	t.Run("mapper ignores nil items", func(t *testing.T) {
		items := make(chan rxgo.Item, 2)
		items <- rxgo.Item{}
		items <- rxgo.Of(1)
		close(items)
		observable := rxgo.FromChannel(items)
		value, err := f(nil, observable)
		require.NoError(t, err)
		require.Equal(t, []int{1}, value)
	})
}

func TestFilterStr(t *testing.T) {
	t.Run("filter str must return nil given item is nil", func(t *testing.T) {
		require.Nil(t, FilterStr(nil))
	})
	t.Run("filter str must return nil given item is empty str", func(t *testing.T) {
		testStr := "     "
		require.Nil(t, FilterStr(&testStr))
		testStr = ""
		require.Nil(t, FilterStr(&testStr))
	})
	t.Run("filter str must return trimmed result", func(t *testing.T) {
		testStr := " hello world "
		result := FilterStr(&testStr)
		require.Equal(t, "hello world", *result)
		testStr = "hello world"
		result = FilterStr(&testStr)
		require.Equal(t, "hello world", *result)
	})
}
