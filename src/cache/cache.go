package cache

import (
	"sync"
	"sync/atomic"
)

const (
	idSegmentShift = 16
	idSegmentMask  = (1 << idSegmentShift) - 1
	idWordShift    = 6
	idSegmentWords = 1 << (idSegmentShift - idWordShift)
)

type idSegment struct {
	words [idSegmentWords]atomic.Uint64
}

// IDSet is a concurrent segmented bit set for positive Discogs identifiers. It allocates one
// 8 KiB segment per observed 65,536-ID range and stores dense identifiers at one bit each.
type IDSet struct {
	segments sync.Map
}

func (s *IDSet) Add(id int32) {
	if id < 1 {
		return
	}
	segmentIndex := id >> idSegmentShift
	value, ok := s.segments.Load(segmentIndex)
	if !ok {
		value, _ = s.segments.LoadOrStore(segmentIndex, new(idSegment))
	}
	segment := value.(*idSegment)
	offset := uint32(id) & idSegmentMask
	segment.words[offset>>idWordShift].Or(uint64(1) << (offset & 63))
}

func (s *IDSet) Contains(id int32) bool {
	if id < 1 {
		return false
	}
	value, ok := s.segments.Load(id >> idSegmentShift)
	if !ok {
		return false
	}
	offset := uint32(id) & idSegmentMask
	word := value.(*idSegment).words[offset>>idWordShift].Load()
	return word&(uint64(1)<<(offset&63)) != 0
}

func (s *IDSet) Reset() {
	s.segments.Range(func(key, _ any) bool {
		s.segments.Delete(key)
		return true
	})
}

func (s *IDSet) AllocatedWordBytes() int64 {
	var segments int64
	s.segments.Range(func(_, _ any) bool {
		segments++
		return true
	})
	return segments * idSegmentWords * 8
}

var (
	// StyleCache stores names used by the current import.
	StyleCache = &sync.Map{}
	// GenreCache stores names used by the current import.
	GenreCache = &sync.Map{}
	ArtistIDs  = &IDSet{}
	LabelIDs   = &IDSet{}
	MasterIDs  = &IDSet{}
)

func ResetIDs() {
	ArtistIDs.Reset()
	LabelIDs.Reset()
	MasterIDs.Reset()
}
