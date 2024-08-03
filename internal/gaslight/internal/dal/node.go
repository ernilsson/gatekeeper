package dal

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const MaxNodeSizeMultiplier = 0.025

type Item struct {
	key   []byte
	value []byte
}

func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

type Node struct {
	id       uint64
	parent   uint64
	children []uint64
	items    []*Item
}

func (n *Node) Find(key []byte) (*Item, bool) {
	for _, item := range n.items {
		if bytes.Equal(item.key, key) {
			return item, true
		}
	}
	return nil, false
}

func (n *Node) Child(key []byte) uint64 {
	var i int
	// find the first index of items where the previous key is not larger than the inserting item
	for i = 0; i < len(n.items); i++ {
		if i == len(n.items) || Compare(key, n.items[i].key) < 0 {
			break
		}
	}
	if i == len(n.items) {
		return n.children[len(n.children)-1]
	}
	return n.children[i]
}

func (n *Node) AddChild(index int, id uint64) {
	if index > len(n.children) {
		panic("tried to add more than `k+1` child nodes")
	}
	if index == len(n.children) {
		n.children = append(n.children, id)
	} else {
		n.children[index] = id
	}
}

func (n *Node) Insert(item *Item) int {
	var i int
	// Find the first index of items where the previous key is not larger than the inserting item
	for i = 0; i < len(n.items); i++ {
		if i == len(n.items) || Compare(item.key, n.items[i].key) < 0 {
			fmt.Println(Compare(n.items[i].key, item.key))
			break
		}
	}
	if i == len(n.items) {
		n.items = append(n.items, item)
	} else {
		n.items = append(n.items[:i+1], n.items[i:]...)
		n.items[i] = item
	}
	return i
}

func (n *Node) Register(key []byte, id uint64) {
	var i int
	// find the first index of items where the previous key is not larger than the inserting item
	for i = 0; i < len(n.items); i++ {
		if i == len(n.items) || Compare(key, n.items[i].key) < 0 {
			break
		}
	}
	if i == len(n.children) {
		n.children = append(n.children, id)
		return
	} else {
		n.children = append(n.children[:i+1], n.children[i:]...)
		n.children[i] = id
	}
}

func (n *Node) Overpopulated() bool {
	return len(n.items) >= 3
	/*var size int
	size += 1 // leaf page header
	size += 2 // length page header
	for _, item := range n.items {
		size += len(item.key)
		size += len(item.value)
		size += 8 // page id
		size += 2 // offset
	}
	size += 8 // final page id
	return float64(size) >= float64(os.Getpagesize())*MaxNodeSizeMultiplier*/
}

func (n *Node) SplitIndex() int {
	return int(float64(len(n.items)) / 2)
}

func Split(n *Node) (*Node, *Node) {
	point := n.SplitIndex()
	a := &Node{
		children: make([]uint64, 0, (len(n.items)/2)+1),
		items:    make([]*Item, 0, len(n.items)/2),
	}
	for _, item := range n.items[:point] {
		a.Insert(item)
	}
	b := &Node{
		children: make([]uint64, 0, (len(n.items)/2)+1),
		items:    make([]*Item, 0, len(n.items)/2),
	}
	for _, item := range n.items[point+1:] {
		b.Insert(item)
	}
	if n.Leaf() {
		// There are no children to assign to the new nodes, hence why we can immediately return them
		return a, b
	}
	point = int(float64(len(n.children)) / 2)
	for i, child := range n.children[:point] {
		a.AddChild(i, child)
	}
	for i, child := range n.children[point:] {
		b.AddChild(i, child)
	}
	return a, b
}

func (n *Node) Leaf() bool {
	return len(n.children) == 0
}

func (n *Node) Parent() bool {
	return len(n.children) > 0
}

func (n *Node) Serialize(buf []byte) {
	head, tail := 0, len(buf)-1

	leaf := uint8(0)
	if n.Leaf() {
		leaf = 1
	}
	buf[head] = leaf
	head += 1
	binary.LittleEndian.PutUint64(buf[head:], n.parent)
	head += 8
	binary.LittleEndian.PutUint16(buf[head:], uint16(len(n.items)))
	head += 2

	for i, item := range n.items {
		if n.Parent() {
			child := n.children[i]
			binary.LittleEndian.PutUint64(buf[head:], child)
			head += 8
		}

		klen, vlen := len(item.key), len(item.value)
		offset := tail - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[head:], uint16(offset))
		head += 2

		tail -= vlen
		copy(buf[tail:], item.value)
		tail -= 1
		buf[tail] = byte(vlen)

		tail -= klen
		copy(buf[tail:], item.key)
		tail -= 1
		buf[tail] = byte(klen)
	}

	if n.Parent() {
		child := n.children[len(n.children)-1]
		binary.LittleEndian.PutUint64(buf[head:], child)
	}
}

func (n *Node) Deserialize(buf []byte) {
	head := 0
	leaf := false
	if buf[head] != byte(0) {
		leaf = true
	}
	parent := !leaf
	head += 1
	n.parent = binary.LittleEndian.Uint64(buf[head : head+8])
	head += 8
	items := binary.LittleEndian.Uint16(buf[head : head+2])
	head += 2

	n.children = make([]uint64, 0, items+1)
	n.items = make([]*Item, 0, items)
	for i := 0; i < int(items); i++ {
		if parent {
			id := binary.LittleEndian.Uint64(buf[head:])
			n.children = append(n.children, id)
			head += 8
		}
		offset := binary.LittleEndian.Uint16(buf[head:])
		head += 2

		klen := uint16(buf[offset])
		offset += 1
		key := make([]byte, klen)
		copy(key, buf[offset:offset+klen])
		offset += klen

		vlen := uint16(buf[offset])
		offset += 1
		value := make([]byte, vlen)
		copy(value, buf[offset:offset+vlen])

		n.items = append(n.items, &Item{
			key:   key,
			value: value,
		})
	}

	if parent {
		child := binary.LittleEndian.Uint64(buf[head:])
		n.children = append(n.children, child)
	}
}
