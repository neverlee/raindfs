package raftlayer

import (
	"fmt"
	"sync"
)

// just for testing
type Sequencer struct {
	Counter      uint32
	sequenceLock sync.Mutex
}

func NewSequencer() (m *Sequencer) {
	s := &Sequencer{Counter: 0}
	return s
}

func (m *Sequencer) String() string {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	return fmt.Sprintf("Squence: %d", m.Counter)
}

func (m *Sequencer) NextId(count uint32) (uint32, uint32) {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	ret := m.Counter
	m.Counter += uint32(count)
	return ret, m.Counter
}

func (m *Sequencer) SetMax(seenValue uint32) uint32 {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	if m.Counter <= seenValue {
		m.Counter = seenValue
	}
	return m.Counter
}

func (m *Sequencer) Peek() uint32 {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	return m.Counter
}
