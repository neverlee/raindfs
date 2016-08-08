package sequence

import (
	"encoding/json"
	"io/ioutil"
	"sync"
)

// just for testing
type Sequencer struct {
	Counter      uint32
	file         string
	sequenceLock sync.Mutex
}

func (s *Sequencer) load() error {
	blob, err := ioutil.ReadFile(s.file)
	if err != nil {
		return err
	}
	return json.Unmarshal(blob, s)
}

func (s *Sequencer) dump() error {
	blob, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(s.file, blob, 0644)
}

func NewSequencer(path string) (m *Sequencer) {
	s := &Sequencer{Counter: 0, file: path}
	s.load()
	s.dump()
	return
}

func (m *Sequencer) NextId(count uint32) (uint32, uint32) {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	ret := m.Counter
	m.Counter += uint32(count)
	m.dump()
	return ret, m.Counter
}

func (m *Sequencer) SetMax(seenValue uint32) {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	if m.Counter <= seenValue {
		m.Counter = seenValue + 1
		m.dump()
	}
}

func (m *Sequencer) Peek() uint32 {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	return m.Counter
}
