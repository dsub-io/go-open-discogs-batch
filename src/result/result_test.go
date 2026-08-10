package result

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultSumPreservesTheFirstError(t *testing.T) {
	firstError := errors.New("first error")
	secondError := errors.New("second error")
	sum := NewResult(2, nil).Sum(NewResult(3, firstError)).Sum(NewResult(4, secondError))

	require.Equal(t, 9, sum.Count())
	require.ErrorIs(t, sum.Err(), firstError)
	require.True(t, sum.IsErr())
	require.False(t, NewResult(0, nil).IsErr())
}

func TestResultSumIgnoresNil(t *testing.T) {
	result := NewResult(2, nil)

	require.Same(t, result, result.Sum(nil))
	require.Equal(t, 2, result.Count())
}
