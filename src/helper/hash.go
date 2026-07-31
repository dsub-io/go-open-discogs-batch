package helper

import (
	"hash/fnv"
	"unicode/utf16"
)

func Fnv32(in []byte) uint32 {
	h := fnv.New32()
	_, _ = h.Write(in)
	return h.Sum32()
}

func Fnv32Str(s string) uint32 {
	return Fnv32([]byte(s))
}

// JavaStringHash reproduces String.hashCode() so Go and Java importers write
// identical relationship keys, including for non-BMP Unicode characters.
func JavaStringHash(s string) int32 {
	var hash int32
	for _, codeUnit := range utf16.Encode([]rune(s)) {
		hash = 31*hash + int32(codeUnit)
	}
	return hash
}
