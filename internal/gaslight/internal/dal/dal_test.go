package dal

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func Test_dalWrite(t *testing.T) {
	file, err := os.OpenFile("./gaslight.db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	d, err := New(file)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	node := &Node{
		id:       d.freelist.id(),
		children: make([]uint64, 0, 4),
		items:    make([]*Item, 0, 3),
	}

	collection := &Collection{
		id:   10,
		name: "principals",
		root: node.id,
		dal:  d,
	}

	if err := d.Serialize(node, node.id); err != nil {
		t.Fatal(err)
	}

	_ = collection.Insert([]byte("Key1"), []byte("Value1"))
	_ = collection.Insert([]byte("Key2"), []byte("Value2"))
	_ = collection.Insert([]byte("Key3"), []byte("Value3"))
	_ = collection.Insert([]byte("Key4"), []byte("Value4"))
	_ = collection.Insert([]byte("Key5"), []byte("Value5"))
	_ = collection.Insert([]byte("Key6"), []byte("Value6"))
	_ = collection.Insert([]byte("Key7"), []byte("Value7"))
	_ = collection.Insert([]byte("Key8"), []byte("Value8"))
	_ = collection.Insert([]byte("Key0"), []byte("Value0"))
	if err := d.Serialize(collection, collection.id); err != nil {
		t.Fatal(err)
	}
}

func Test_dalRead(t *testing.T) {
	file, err := os.OpenFile("./gaslight.db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	d, err := Load(file)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	collection := &Collection{
		dal: d,
	}
	if err := d.Deserialize(collection, 10); err != nil {
		t.Fatal(err)
	}
	banana, err := collection.Find([]byte("Key7"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%s, %s\n", banana.key, banana.value)
}

func Test_print(t *testing.T) {
	file, err := os.OpenFile("./gaslight.db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	d, err := Load(file)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	collection := &Collection{
		dal: d,
	}
	if err := d.Deserialize(collection, 10); err != nil {
		t.Fatal(err)
	}
	root := &Node{}
	if err := d.Deserialize(root, collection.root); err != nil {
		t.Fatal(err)
	}
	traverse(collection.dal, root, 0)
}

func traverse(dal *DAL, n *Node, level int) {
	builder := &strings.Builder{}
	for i := 0; i < level; i++ {
		builder.WriteString(" ")
	}
	for _, item := range n.items {
		fmt.Printf("%s%s\n", builder.String(), item.key)
	}
	if n.Parent() {
		for _, child := range n.children {
			node := &Node{}
			if err := dal.Deserialize(node, child); err != nil {
				panic(err)
			}
			traverse(dal, node, level+2)
			fmt.Println()
		}
	}
}
