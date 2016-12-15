package raftlayer

import (
	"encoding/json"
	"errors"
	"io"
	"raindfs/sequence"

	"github.com/hashicorp/raft"
)

var (
	errBadAction = errors.New("bad action")
)

type Request struct {
	Action int
	Key    uint32
}

func NewRequest(data []byte) (*Request, error) {
	var req Request
	err := json.Unmarshal(data, &req)
	return &req, err
}

func (r *Request) Encode() []byte {
	blob, _ := json.Marshal(r)
	return blob
}

type FSM struct {
	seq *sequence.Sequencer
}

func NewFSM() *FSM {
	fsm := FSM{
		seq: sequence.NewSequencer(),
	}
	return &fsm
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	req, err := NewRequest(l.Data)
	if err != nil {
		return err
	}
	switch req.Action {
	case 0: // OpNext
		f.seq.NextId(req.Key)
		return nil
	case 1: // OpSetMax
		f.seq.SetMax(req.Key)
		return nil
	default:
		return errBadAction
	}
}

// fsmSnapshot implement FSMSnapshot interface
type fsmSnapshot struct {
	Sequence uint32
	Cluster  map[string]string
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	snap := fsmSnapshot{
		Sequence: f.seq.Peek(),
		//Cluster: make(map[string]string),
	}

	return &snap, nil
}

func (f *FSM) Restore(r io.ReadCloser) error {
	defer r.Close()

	var snap fsmSnapshot
	dec := json.NewDecoder(r)
	if err := dec.Decode(&snap); err != nil {
		return err
	}

	f.seq.SetMax(snap.Sequence)
	return nil
}

// First, walk all kvs
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	blob, _ := json.Marshal(f)
	sink.Write(blob)
	//sink.Cancel()
	sink.Close()
	return nil
}

func (f *fsmSnapshot) Release() {
	// f.snapshot.Release()
}
