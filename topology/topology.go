package topology

import (
	"raindfs/sequence"
	"raindfs/storage"
)

type Topology struct {
	nodemap      *DataNodeMap
	volumeLayout *VolumeLayout

	pulse    int
	Sequence *sequence.Sequencer

	chanDeadDataNodes      chan *DataNode
	chanRecoveredDataNodes chan *DataNode
	chanFullVolumes        chan storage.VolumeInfo
}

func NewTopology(seq *sequence.Sequencer, pulse int) *Topology {
	t := &Topology{}
	t.pulse = pulse
	t.Sequence = seq

	t.nodemap = NewDataNodeMap()
	t.volumeLayout = NewVolumeLayout()

	t.chanDeadDataNodes = make(chan *DataNode)
	t.chanRecoveredDataNodes = make(chan *DataNode)
	t.chanFullVolumes = make(chan storage.VolumeInfo)

	return t
}

func (t *Topology) Lookup(vid storage.VolumeId) []*DataNode {
	return t.volumeLayout.Lookup(vid)
}

func (t *Topology) NextVolumeId() storage.VolumeId {
	_, r := t.Sequence.NextId(1)
	return storage.VolumeId(r)
}

func (t *Topology) HasWritableVolume() bool {
	return t.volumeLayout.GetActiveVolumeCount() > 0
}

//func (t *Topology) PickForWrite(count uint64) (string, uint64, *DataNode, error) {
//	vid, count, datanodes, err := t.volumeLayout.PickForWrite(count, option)
//	if err != nil || datanodes.Length() == 0 {
//		return "", 0, nil, errors.New("No writable volumes available!")
//	}
//	fileId, count := t.Sequence.NextFileId(count)
//	return storage.NewFileId(*vid, fileId, rand.Uint32()).String(), count, datanodes.Head(), nil
//}

//func (t *Topology) ProcessJoinMessage(joinMessage *operation.JoinMessage) {
//	dn := t.FindDataNode(joinMessage.Ip, int(joinMessage.Port))
//	if joinMessage.IsInit && dn != nil {
//		t.UnRegisterDataNode(dn)
//	}
//	dn = t.GetOrCreateDataNode(joinMessage.Ip,
//		int(joinMessage.Port),
//		int(joinMessage.MaxVolumeCount))
//	var volumeInfos []storage.VolumeInfo
//	for _, v := range joinMessage.Volumes {
//		if vi, err := storage.NewVolumeInfo(v); err == nil {
//			volumeInfos = append(volumeInfos, vi)
//		} else {
//			glog.V(0).Infoln("Fail to convert joined volume information:", err.Error())
//		}
//	}
//	deletedVolumes := dn.UpdateVolumes(volumeInfos)
//	for _, v := range volumeInfos {
//		t.RegisterVolumeLayout(v, dn)
//	}
//	for _, v := range deletedVolumes {
//		t.UnRegisterVolumeLayout(v, dn)
//	}
//}

func (t *Topology) ToMap() interface{} {
	m := make(map[string]interface{})
	//m["Max"] = t.GetMaxVolumeCount()
	m["DataNodes"] = t.nodemap.ToMap()
	//m["layouts"] = layouts
	return m
}

func (t *Topology) ToVolumeMap() interface{} {
	m := make(map[string]interface{})
	//m["Max"] = t.GetMaxVolumeCount()
	//dcs := make(map[string]interface{})
	//for _, dn := range t.nodes {
	//	var volumes []interface{}
	//	for _, v := range dn.GetVolumes() {
	//		volumes = append(volumes, v)
	//	}
	//	dcs[dn.Url()] = volumes
	//}
	//m["DataNodes"] = dcs
	return m
}

func (t *Topology) GetMaxVolumeId() storage.VolumeId {
	r := t.Sequence.Peek()
	return storage.VolumeId(r)
}

//func (t *Topology) GetVolumeCount() int
//func (t *Topology) GetActiveVolumeCount() int
//func (t *Topology) GetMaxVolumeCount() int
