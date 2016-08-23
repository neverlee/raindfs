package sequence

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
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
	path := s.file
	blob, err := json.Marshal(s)
	if err != nil {
		return err
	}

	newpath := path + "_new"
	if err = ioutil.WriteFile(newpath, blob, 0644); err != nil {
		return err
	}
	return os.Rename(newpath, path)
}

func NewSequencer(path string) (m *Sequencer) {
	s := &Sequencer{Counter: 0, file: path}
	s.load()
	s.dump()
	return s
}

func (m *Sequencer) String() string {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	return fmt.Sprintf("Squence: <%s> %d", m.file, m.Counter)
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
