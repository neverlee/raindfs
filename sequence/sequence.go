package sequence

type Sequencer interface {
	NextId(count uint32) (uint32, uint32)
	SetMax(uint32)
	Peek() uint32
}
