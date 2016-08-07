package sequence

import (
	"sync"
)

// just for testing
type MemorySequencer struct {
	counter      uint32
	sequenceLock sync.Mutex
}

func NewMemorySequencer() (m *MemorySequencer) {
	m = &MemorySequencer{counter: 1}
	return
}

func (m *MemorySequencer) NextId(count uint32) (uint32, uint32) {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	ret := m.counter
	m.counter += uint32(count)
	return ret, m.counter
}

func (m *MemorySequencer) SetMax(seenValue uint32) {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	if m.counter <= seenValue {
		m.counter = seenValue + 1
	}
}

func (m *MemorySequencer) Peek() uint32 {
	return m.counter
}
