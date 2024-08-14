package dal

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	EmptyNodeID = 0
)

var (
	ErrItemNotFound  = errors.New("item not found")
	ErrNoParentFound = errors.New("node is root")
)

type Collection struct {
	id   uint64
	name string
	root uint64
	dal  *DAL
}

func (c *Collection) Serialize(buf []byte) {
	head := serializer{
		direction: forwards,
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
	n, err := c.find(key, c.root)
	if err != nil {
		return nil, err
	}
	item, _ := n.Find(key)
	return item, nil
}

func (c *Collection) find(key []byte, id uint64) (*Node, error) {
	node := &Node{}
	if err := c.dal.Deserialize(node, id); err != nil {
		return nil, err
	}
	_, found := node.Find(key)
	if found {
		return node, nil
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
		case errors.Is(err, ErrNoParentFound):
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
		return nil, ErrNoParentFound
	}
	parent := &Node{}
	if err := c.dal.Deserialize(parent, n.parent); err != nil {
		return nil, err
	}
	parent.id = n.parent
	return parent, nil
}

func (c *Collection) LeftSibling(node *Node) (*Node, error) {
	parent, err := c.Parent(node)
	if err != nil {
		return nil, err
	}
	index, err := parent.ChildIndex(node.id)
	if err != nil {
		return nil, err
	}
	if index == 0 {
		return nil, nil
	}
	sibling := &Node{}
	if err := c.dal.Deserialize(sibling, parent.children[index-1]); err != nil {
		return nil, err
	}
	return sibling, nil
}

func (c *Collection) RightSibling(node *Node) (*Node, error) {
	parent, err := c.Parent(node)
	if err != nil {
		return nil, err
	}
	index, err := parent.ChildIndex(node.id)
	if err != nil {
		return nil, err
	}
	if index == len(parent.children)-1 {
		return nil, nil
	}
	sibling := &Node{}
	if err := c.dal.Deserialize(sibling, parent.children[index+1]); err != nil {
		return nil, err
	}
	return sibling, nil
}

func (c *Collection) merge(dst, src *Node) {
	for _, item := range src.items {
		dst.Insert(item)
	}
	clear(src.items)
}

func (c *Collection) Delete(key []byte) error {
	_, err := c.delete(key)
	if err != nil {
		return err
	}
	// TODO: Implement the re-balancing of the tree after deletion
	return c.balance(nil)
}

func (c *Collection) balance(node *Node) error {
	panic("not implemented")
}

func (c *Collection) rotateRight(parent, node, sibling *Node) error {
	item := node.items[len(node.items)-1]
	node.items = node.items[:len(node.items)-1]
	index, err := parent.ChildIndex(sibling.id)
	if err != nil {
		return err
	}
	itm := int(math.Max(float64(index-1), 0))
	parentItem := parent.items[itm]
	parent.items[itm] = item
	sibling.items = append([]*Item{parentItem}, sibling.items...)

	if node.Parent() {
		shift := node.children[len(node.children)-1]
		node.children = node.children[:len(node.children)-1]
		sibling.children = append([]uint64{shift}, sibling.children...)
	}
	return nil
}

func (c *Collection) rotateLeft(parent, node, sibling *Node) error {
	item := sibling.items[0]
	sibling.items = sibling.items[1:]
	itm, err := parent.ChildIndex(sibling.id)
	if err != nil {
		return err
	}
	if itm == len(parent.children)-1 {
		itm = len(parent.items) - 1
	}
	parentItem := parent.items[itm]
	parent.items[itm] = item
	node.items = append(node.items, parentItem)

	if node.Parent() {
		shift := sibling.children[0]
		sibling.children = sibling.children[1:]
		node.children = append(sibling.children, shift)
	}
	return nil
}

// delete hides the complexity of the actual deletion of a key and separates the deletion-specific operations from
// re-balancing the tree.
func (c *Collection) delete(key []byte) ([]Node, error) {
	node, err := c.find(key, c.root)
	if err != nil {
		return nil, err
	}
	if node.Leaf() {
		err := node.Delete(key)
		if err != nil {
			return nil, err
		}
		return nil, c.dal.Serialize(node, node.id)
	}

	affected := make([]Node, 0, 3)
	affected = append(affected, *node)
	index, err := node.ItemIndex(key)
	if err != nil {
		return nil, err
	}
	child := &Node{}
	if err := c.dal.Deserialize(child, node.children[index]); err != nil {
		return nil, err
	}
	affected = append(affected, *child)
	if child.Parent() {
		if err := c.dal.Deserialize(child, child.children[len(child.children)-1]); err != nil {
			return nil, err
		}
		affected = append(affected, *child)
	}
	node.items[index] = child.items[len(child.items)-1]
	child.items = child.items[:len(child.items)-1]
	for _, ch := range affected {
		if err := c.dal.Serialize(&ch, ch.id); err != nil {
			return nil, err
		}
	}
	return affected, nil
}
