package batch

import (
	"math/big"
	"testing"
)

var (
	benchmarkCanonicalQuantity string
	benchmarkIntegerQuantity   *int32
)

func BenchmarkReleaseFormatQuantityParsing(b *testing.B) {
	values := map[string]string{
		"typical":   "0002",
		"oversized": oversizedReleaseFormatQuantity,
	}
	for name, value := range values {
		b.Run("linear/"+name, func(b *testing.B) {
			for range b.N {
				canonical, integer, err := canonicalReleaseFormatQuantity(value)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkCanonicalQuantity = canonical
				benchmarkIntegerQuantity = integer
			}
		})
		b.Run("big-int-baseline/"+name, func(b *testing.B) {
			for range b.N {
				canonical, integer := benchmarkBigIntQuantity(value)
				benchmarkCanonicalQuantity = canonical
				benchmarkIntegerQuantity = integer
			}
		})
	}
}

func benchmarkBigIntQuantity(value string) (string, *int32) {
	parsed, valid := new(big.Int).SetString(value, 10)
	if !valid || parsed.Sign() < 0 {
		return "", nil
	}
	canonical := parsed.String()
	if parsed.BitLen() > 31 {
		return canonical, nil
	}
	integer := int32(parsed.Int64())
	return canonical, &integer
}
