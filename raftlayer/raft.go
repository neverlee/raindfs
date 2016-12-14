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

func NewRaftLayer(l net.Listener) *RaftLayer {
	// advertise net.Addr,
	return &RaftLayer{
		listener: l,
		// advertise: advertise,
	}
}

func (t *RaftLayer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	_, err = conn.Write([]byte{RaftProto})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
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

func NewRaft(path string, fsm raft.FSM, trans raft.Transport, clusters []string, singleNode bool, interval time.Duration, threshold uint64) (*raft.Raft, error) {
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

	peerStore := raft.NewJSONPeers(path, trans)

	//if c.Raft.ClusterState == ClusterStateNew {
	//	log.Infof("cluster state is new, use new cluster config")
	//	r.peerStore.SetPeers(peers)
	//} else {
	ps, err := peerStore.Peers()
	if err != nil {
		fmt.Println("get store peers error %v", err)
		return nil, err
	}
	for _, peer := range clusters {
		ps = raft.AddUniquePeer(ps, peer)
	}

	fmt.Println("setpeers", ps)
	peerStore.SetPeers(ps)
	//}

	//if peers, _ := r.peerStore.Peers(); len(peers) <= 1 {
	//	cfg.EnableSingleNode = true
	//}

	raftConfig := raft.DefaultConfig()
	raftConfig.SnapshotInterval = time.Duration(interval)
	raftConfig.SnapshotThreshold = threshold
	raftConfig.EnableSingleNode = singleNode

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
