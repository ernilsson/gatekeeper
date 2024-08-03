package dal

import (
	"encoding/binary"
	"errors"
)

const (
	EmptyNodeID = 0
)

var (
	ErrItemNotFound = errors.New("item not found")
	ErrNodeIsRoot   = errors.New("node is root")
)

type Collection struct {
	id   uint64
	name string
	root uint64
	dal  *DAL
}

func (c *Collection) Serialize(buf []byte) {
	head := serializer{
		direction: 1,
		buffer:    buf,
	}
	head.PutUint64(c.root)
	head.PutUint16(uint16(len(c.name)))
	head.Put([]byte(c.name))
}

func (c *Collection) Deserialize(buf []byte) {
	head := 0
	c.root = binary.LittleEndian.Uint64(buf[head:])
	head += 8
	length := binary.LittleEndian.Uint16(buf[head:])
	head += 2
	name := make([]byte, length)
	copy(name, buf[head:])
	c.name = string(name)
}

func (c *Collection) Find(key []byte) (*Item, error) {
	return c.find(key, c.root)
}

func (c *Collection) find(key []byte, id uint64) (*Item, error) {
	node := &Node{}
	if err := c.dal.Deserialize(node, id); err != nil {
		return nil, err
	}
	item, found := node.Find(key)
	if found {
		return item, nil
	}
	if node.Leaf() {
		return nil, ErrItemNotFound
	}
	return c.find(key, node.Child(key))
}

func (c *Collection) Insert(key, val []byte) error {
	item := &Item{
		key:   key,
		value: val,
	}
	node := &Node{
		id: c.root,
	}
	if err := c.dal.Deserialize(node, c.root); err != nil {
		return err
	}
	for node.Parent() {
		parent := node.id
		child := node.Child(item.key)
		if err := c.dal.Deserialize(node, node.Child(item.key)); err != nil {
			return err
		}
		node.id = child
		node.parent = parent
	}
	node.Insert(item)
	if node.Overpopulated() {
		return c.Split(node)
	}
	return c.dal.Serialize(node, node.id)
}

func (c *Collection) Split(n *Node) error {
	parent, err := c.Parent(n)
	if err != nil {
		switch {
		case errors.Is(err, ErrNodeIsRoot):
			// This node is the root of the tree, by splitting we will create a new root node to hold references to the
			// split node segments
			parent = &Node{
				id:       c.dal.freelist.id(),
				children: make([]uint64, 0, 4),
				items:    make([]*Item, 0, 3),
			}
			// Since a new root node must be created we shall also make sure to update the collections root node
			// identifier to match the new root node
			c.root = parent.id
		default:
			return err
		}
	}
	a, b, promoted := Split(n)
	ptr := parent.Insert(promoted)
	// Since two new nodes are created by split we shall release the page on which the split node was stored on
	c.dal.freelist.release(n.id)

	a.parent, b.parent = parent.id, parent.id
	a.id, b.id = c.dal.freelist.id(), c.dal.freelist.id()
	// The page identifiers need to be added to the parent at the correct index to ensure traversal of the tree
	parent.AddChild(ptr, a.id)
	parent.AddChild(ptr+1, b.id)

	if err := c.dal.Serialize(parent, parent.id); err != nil {
		return err
	}
	if err := c.dal.Serialize(a, a.id); err != nil {
		return err
	}
	if err := c.dal.Serialize(b, b.id); err != nil {
		return err
	}
	// If adding another key to the parent caused it to overpopulate we need to recursively apply the same operation to
	// the parent, either until the parent is no longer overpopulated or until the root has been split.
	if parent.Overpopulated() {
		return c.Split(parent)
	}
	return nil
}

func (c *Collection) Parent(n *Node) (*Node, error) {
	if n.parent == EmptyNodeID {
		return nil, ErrNodeIsRoot
	}
	parent := &Node{}
	if err := c.dal.Deserialize(parent, n.parent); err != nil {
		return nil, err
	}
	parent.id = n.parent
	return parent, nil
}
