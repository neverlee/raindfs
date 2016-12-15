package topology

import (
	"errors"
	"math/rand"
	"time"

	"raindfs/operation"
	"raindfs/raftlayer"
	"raindfs/storage"
	"raindfs/util"

	"github.com/hashicorp/raft"
	"github.com/neverlee/glog"
)

type Topology struct {
	nodemap      *DataNodeMap
	volumeLayout *VolumeLayout

	fsm *raftlayer.FSM

	pulse int

	raft *raft.Raft
	//chanDeadDataNodes, chanRecoveredDataNodes *DataNode, chanFullVolumes storage.VolumeInfo
}

func NewTopology(raft *raft.Raft, fsm *raftlayer.FSM, pulse int) *Topology {
	t := &Topology{}
	t.raft = raft
	t.fsm = fsm
	t.pulse = pulse

	t.nodemap = NewDataNodeMap()
	t.volumeLayout = NewVolumeLayout()

	return t
}

func (t *Topology) IsLeader() bool {
	return t.raft.State() != raft.Leader
}

func (t *Topology) PeekVolumeId() storage.VolumeId {
	return storage.VolumeId(t.fsm.Peek())
}

func (t *Topology) UpVolumeId(i uint32) (storage.VolumeId, error) {
	if t.raft.State() != raft.Leader {
		return 0, errors.New("not leader")
	}

	req := raftlayer.Request{Action: 0, Key: i}
	f := t.raft.Apply(req.Encode(), time.Second*4)
	err := f.Error()
	if err == nil {
		ri := f.Response().(uint32)
		return storage.VolumeId(ri), nil
	}
	return 0, err
}

func (t *Topology) StartRefreshWritableVolumes() {
	glog.V(0).Infoln("StartRefreshWritableVolumes")
	go func() {
		for {
			if t.IsLeader() {
				freshThreshHold := time.Now().Unix() - int64(3*t.pulse) //3 times of sleep interval
				t.nodemap.CollectDeadNode(freshThreshHold)
			}
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	go func() {
		for {
			if t.IsLeader() {
				nodes := t.nodemap.CollectNodeNeedNewVolume()
				for i := 0; i < len(nodes); i += 2 {
					ivid, uverr := t.UpVolumeId(1)
					if uverr != nil {
						continue
					}

					vid := storage.VolumeId(ivid)
					if ainfo, aerr := nodes[i].AssignVolume(vid); aerr == nil {
						glog.V(0).Infoln("Assign New Volume", vid, nodes[i].Url(), ainfo, aerr)
						t.volumeLayout.RegisterVolume(ainfo, nodes[i])
					}
					if binfo, berr := nodes[i+1].AssignVolume(vid); berr == nil {
						glog.V(0).Infoln("Assign New Volume", vid, nodes[i+1].Url(), binfo, berr)
						t.volumeLayout.RegisterVolume(binfo, nodes[i+1])
					}
				}
			}
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()

	//go func() {
	//	c := time.Tick(15 * time.Minute)
	//	for _ = range c { if t.IsLeader() { t.Vacuum() } }
	//}()
	//for {
	//	select {
	//	case v := <-t.chanFullVolumes:
	//		t.SetVolumeCapacityFull(v)
	//	case dn := <-t.chanRecoveredDataNodes:
	//		t.RegisterRecoveredDataNode(dn)
	//	}
	//}
}

func (t *Topology) Lookup(vid storage.VolumeId) []*DataNode {
	return t.volumeLayout.Lookup(vid)
}

func (t *Topology) HasWritableVolume() bool {
	return t.volumeLayout.ActiveVolumeCount() > 0
}

func (t *Topology) PickForWrite() (storage.VolumeId, *VolumeLocationList, error) {
	//return t.volumeLayout.PickForWrite()
	if vid, datanodes, err := t.volumeLayout.PickForWrite(); err == nil {
		return vid, datanodes, err
	}
	wnodes := t.nodemap.GetWritableNodes()
	if len(wnodes) >= replicate {
		idx := util.RandTwo(len(wnodes))
		ivid, uperr := t.UpVolumeId(1)
		if uperr != nil {
			return 0, nil, uperr
		}
		vid := storage.VolumeId(ivid)
		var nodelist []*DataNode
		for _, id := range idx {
			vinfo, aerr := wnodes[id].AssignVolume(vid)
			if aerr != nil {
				return 0, nil, aerr
			}
			t.volumeLayout.RegisterVolume(vinfo, wnodes[id])
			nodelist = append(nodelist, wnodes[id])
		}
		loc := &VolumeLocationList{list: nodelist}
		return vid, loc, nil
	}
	return 0, nil, errors.New("No writable node")
}

func (t *Topology) RegisterRecoveredDataNode(dn *DataNode) {
	for _, v := range dn.GetVolumes() {
		if t.volumeLayout.isWritable(&v) {
			t.volumeLayout.SetVolumeAvailable(dn, v.Id)
		}
	}
}

func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.GetVolumes() {
		glog.V(0).Infoln("Removing Volume", v.Id, "from the dead volume server", dn)
		t.volumeLayout.SetVolumeUnavailable(dn, v.Id)
	}
	t.nodemap.UnlinkChildNode(dn.Url())
}

func (t *Topology) ProcessJoinMessage(joinMessage *operation.JoinMessage) *operation.JoinMessage {
	dn := t.nodemap.FindDataNode(joinMessage.Addr)
	// if joinMessage.IsInit && dn != nil { t.UnRegisterDataNode(dn) }
	// 处理reconvered
	dn = t.nodemap.GetOrCreateDataNode(joinMessage.Addr, int(joinMessage.MaxVolumeCount))
	dn.SetFreeSpace(int(joinMessage.FreeSpace))

	var volumeInfos []storage.VolumeInfo
	for _, v := range joinMessage.Volumes {
		vi := *storage.NewVolumeInfo(v)
		volumeInfos = append(volumeInfos, vi)
	}
	deletedVolumes := dn.UpdateVolumes(volumeInfos)
	for _, v := range volumeInfos {
		t.volumeLayout.RegisterVolume(&v, dn)
	}
	for _, v := range deletedVolumes {
		t.volumeLayout.UnRegisterVolume(&v, dn)
	}

	// 返回主master ip
	var jmsg operation.JoinMessage
	return &jmsg
}

type TopologyData struct {
	DataNodes []*DataNodeData
	Layouts   *VolumeLayoutData
}

func (t *Topology) ToData() *TopologyData {
	return &TopologyData{
		DataNodes: t.nodemap.ToData(),
		Layouts:   t.volumeLayout.ToData(),
	}
}
