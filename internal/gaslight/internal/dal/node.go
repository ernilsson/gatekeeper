package dal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
)

const MaxNodeSizeMultiplier = .9

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

// Find looks for a key matching the provided one within the items of the node and returns it if found. The returned
// boolean will be true if a match was found, otherwise it will be false. If no item in the node contains the same key
// as the one provided then the returned item will be nil.
func (n *Node) Find(key []byte) (*Item, bool) {
	for _, item := range n.items {
		if bytes.Equal(item.key, key) {
			return item, true
		}
	}
	return nil, false
}

// Child returns the page id of the child which is assigned values under the provided key. See it as a way to find which
// node should be traversed next in order to find the item for a given key.
func (n *Node) Child(key []byte) uint64 {
	var i int
	// Find the first index of items where the previous key is not larger than the inserting item
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

// AddChild ensures that the provided index will be the index of the provided child id if successful. The provided index
// need not be within the range of the children slice, but it must not exceed it by more than one, if it does then the
// function will panic.
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

// Insert inserts the provided item in sorted order amongst the already existing items of the node.
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

// Overpopulated returns true if the node currently takes up too much disk space and should be split into more than one
// node.
func (n *Node) Overpopulated() bool {
	var size int
	size += 1 // leaf page header
	size += 2 // length page header
	for _, item := range n.items {
		size += len(item.key)
		size += len(item.value)
		size += 8 // page id
		size += 2 // offset
	}
	size += 8 // final page id
	return float64(size) >= float64(os.Getpagesize())*MaxNodeSizeMultiplier
}

// Split creates two nodes from n. The first node will contain items and children from the first half of n and the
// second node will contain items and children from the second half. The item located directly at the split index is not
// included in either of the new nodes. The promoted item is instead returned to the caller to be passed into a parent
// node.
func Split(n *Node) (*Node, *Node, *Item) {
	point := int(float64(len(n.items)) / 2)
	promoted := n.items[point]
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
		// There are no children to assign to the new nodes, hence why we can immediately return
		return a, b, promoted
	}
	point = int(math.Round(float64(len(n.children)) / 2))
	for i, child := range n.children[:point] {
		a.AddChild(i, child)
	}
	for i, child := range n.children[point:] {
		b.AddChild(i, child)
	}
	return a, b, promoted
}

// Leaf returns true if the node does not have any children.
func (n *Node) Leaf() bool {
	return len(n.children) == 0
}

// Parent returns true if the node has at least one child.
func (n *Node) Parent() bool {
	return len(n.children) > 0
}

func (n *Node) Serialize(buf []byte) {
	head := serializer{
		direction: forwards,
		buffer:    buf,
	}
	tail := serializer{
		cursor:    len(buf) - 1,
		direction: backwards,
		buffer:    buf,
	}

	leaf := uint8(0)
	if n.Leaf() {
		leaf = 1
	}
	head.PutUint8(leaf)
	head.PutUint64(n.parent)
	head.PutUint16(uint16(len(n.items)))

	for i, item := range n.items {
		if n.Parent() {
			head.PutUint64(n.children[i])
		}

		offset := tail.cursor - len(item.key) - len(item.value) - 2
		head.PutUint16(uint16(offset))

		tail.Put(item.value)
		tail.PutUint8(uint8(len(item.value)))
		tail.Put(item.key)
		tail.PutUint8(uint8(len(item.key)))
	}

	if n.Parent() {
		head.PutUint64(n.children[len(n.children)-1])
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
