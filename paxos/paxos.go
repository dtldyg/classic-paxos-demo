package paxos

type Node interface {
	ID() string
}

type Paxos struct {
	node Node
}

func NewPaxos(node Node) *Paxos {
	return &Paxos{
		node: node,
	}
}

func (p *Paxos) IsMaster() bool {
}

func (p *Paxos) WhoMaster() string {
}
