package raftlayer

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	RaftProto = 0
)

type RaftLayer struct {
	listener  net.Listener
	advertise net.Addr
}

func NewRaftLayer(l net.Listener, addr net.Addr) *RaftLayer {
	// advertise net.Addr,
	return &RaftLayer{
		listener:  l,
		advertise: addr,
	}
}

func (t *RaftLayer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", address, timeout)
}

// Accept implements the net.Listener interface.
func (t *RaftLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close implements the net.Listener interface.
func (t *RaftLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr implements the net.Listener interface.
func (t *RaftLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}

func NewRaft(path string, fsm raft.FSM, trans raft.Transport, clusters []string, interval time.Duration, threshold uint64) (*raft.Raft, error) {
	raftLogDir := filepath.Join(path, "log.db")
	raftMetaDir := filepath.Join(path, "meta.db")

	logStore, err := raftboltdb.NewBoltStore(raftLogDir)
	if err != nil {
		return nil, err
	}

	metaStore, err := raftboltdb.NewBoltStore(raftMetaDir)
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(path, 3, os.Stderr)
	if err != nil {
		return nil, err
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.SnapshotInterval = time.Duration(interval)
	raftConfig.SnapshotThreshold = threshold

	peerStore := raft.NewJSONPeers(path, trans)

	if len(clusters) > 1 {
		ps, err := peerStore.Peers()
		if err != nil {
			fmt.Println("get store peers error %v", err)
			return nil, err
		}
		for _, peer := range clusters {
			ps = raft.AddUniquePeer(ps, peer)
		}

		peerStore.SetPeers(ps)
	} else {
		raftConfig.EnableSingleNode = true
	}

	err = raft.ValidateConfig(raftConfig)
	if err != nil {
		return nil, err
	}
	return raft.NewRaft(
		raftConfig,
		fsm,
		logStore,
		metaStore,
		snapshotStore,
		peerStore,
		trans,
	)
}
