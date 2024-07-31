package dal

import (
	"encoding/binary"
	"errors"
)

var ErrItemNotFound = errors.New("item not found")

type Collection struct {
	id   uint64
	name string
	root uint64
	dal  *DAL
}

func (c *Collection) Serialize(buf []byte) {
	head := 0
	binary.LittleEndian.PutUint64(buf[head:], c.root)
	head += 8
	binary.LittleEndian.PutUint16(buf[head:], uint16(len(c.name)))
	head += 2
	copy(buf[head:], c.name)
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

func (c *Collection) Insert(item *Item) error {
	node := &Node{
		id: c.root,
	}
	if err := c.dal.Deserialize(node, c.root); err != nil {
		return err
	}
	for node.Parent() {
		child := node.Child(item.key)
		if err := c.dal.Deserialize(node, node.Child(item.key)); err != nil {
			return err
		}
		node.id = child
	}
	node.Insert(item)
	if node.Overpopulated() {
		return c.Split(node)
	}
	return c.dal.Serialize(node, node.id)
}

func (c *Collection) Split(n *Node) error {
	parent := &Node{}
	if n.parent == 0 {
		// This node is the root of the tree, by splitting we will create a new root node to hold references to the
		// split node segments
		parent = &Node{
			id:       c.dal.freelist.id(),
			children: make([]uint64, 0, 4),
			items:    make([]*Item, 0, 3),
		}
		c.root = parent.id
	} else if err := c.dal.Deserialize(parent, n.parent); err != nil {
		return err
	}
	a, b := Split(n)
	a.parent, b.parent = parent.id, parent.id
	a.id, b.id = c.dal.freelist.id(), c.dal.freelist.id()
	item, _ := a.Find(a.MaxKey())
	parent.Register(item.key, a.id)
	item, _ = b.Find(b.MinKey())
	parent.Register(item.key, b.id)

	if err := c.dal.Serialize(parent, parent.id); err != nil {
		return err
	}
	if err := c.dal.Serialize(a, a.id); err != nil {
		return err
	}
	if err := c.dal.Serialize(b, b.id); err != nil {
		return err
	}
	// The page that was split is released to free up the space
	c.dal.freelist.release(n.id)
	if parent.Overpopulated() {
		return c.Split(parent)
	}
	return nil
}
