package raftlayer

import (
	"errors"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

func strInSlice(ay []string, a string) bool {
	for _, e := range ay {
		if e == a {
			return true
		}
	}
	return false
}

type RaftServer struct {
	clusters   []string
	metaFolder string

	raft      *raft.Raft
	raftLayer *RaftLayer
	raftTrans *raft.NetworkTransport

	fsm      *FSM
	listener net.Listener
}

func NewRaftServer(listener net.Listener, addr string, clusters []string, metaFolder string) *RaftServer {
	rs := &RaftServer{
		clusters:   clusters,
		metaFolder: metaFolder,
		listener:   listener,
	}

	if len(clusters) < 1 {
		clusters = []string{addr}
	}
	if !strInSlice(clusters, addr) {
		return nil
	}

	advertise, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil
	}
	layer := NewRaftLayer(listener, advertise)
	trans := raft.NewNetworkTransport(
		layer,
		len(clusters),
		time.Second,
		os.Stderr,
	)

	fsm := NewFSM()

	// setup raft
	raft, err := NewRaft(metaFolder, fsm, trans, clusters, time.Second, 5)
	if err != nil {
		return nil
	}

	rs.raftLayer = layer
	rs.raftTrans = trans
	rs.raft = raft
	rs.fsm = fsm

	return rs
}

func (rs *RaftServer) Leader() string {
	return ""
}

func (rs *RaftServer) Peers() []string {
	return rs.clusters
}

func (rs *RaftServer) IsLeader() bool {
	return rs.raft.State() != raft.Leader
}

func (rs *RaftServer) GetVid() uint32 {
	return rs.fsm.Peek()
}

func (rs *RaftServer) Apply(action int, key uint32) (uint32, error) {
	if rs.raft.State() != raft.Leader {
		return 0, errors.New("not leader")
	}

	req := Request{Action: action, Key: key}
	f := rs.raft.Apply(req.Encode(), time.Second*4)
	err := f.Error()
	if err == nil {
		ri := f.Response().(uint32)
		return ri, nil
	}
	return 0, err
}

func (rs *RaftServer) Close() error {
	rs.Close()
	rs.raftTrans.Close()
	ret := rs.raft.Shutdown()
	// wait raft shutdown
	err := ret.Error()
	rs.listener.Close()
	return err
}
