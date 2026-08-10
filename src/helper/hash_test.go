package helper

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFnv32(t *testing.T) {
	t.Run("result must be consistent", func(t *testing.T) {
		testBytes := []byte("test bytes")
		res := Fnv32(testBytes)
		for i := 0; i < 10; i++ {
			require.Equal(t, res, Fnv32(testBytes))
		}
	})
}

func TestFnv32Str(t *testing.T) {
	t.Run("result must be equal from Fnv32", func(t *testing.T) {
		testStr := "test bytes"
		testBytes := []byte(testStr)
		bRes := Fnv32(testBytes)
		sRes := Fnv32Str(testStr)
		require.Equal(t, bRes, sRes)
	})
}

func TestJavaStringHashMatchesJavaSemantics(t *testing.T) {
	require.Equal(t, int32(0), JavaStringHash(""))
	require.Equal(t, int32(65), JavaStringHash("A"))
	require.Equal(t, int32(1_835_364), JavaStringHash("A😀"))
}
