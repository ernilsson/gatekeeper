package dal

import (
	"fmt"
	"os"
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
		id:   6,
		name: "principals",
		root: node.id,
		dal:  d,
	}

	if err := d.Serialize(node, node.id); err != nil {
		t.Fatal(err)
	}

	err = collection.Insert(&Item{
		key:   []byte("rasmus"),
		value: []byte("developer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = collection.Insert(&Item{
		key:   []byte("peter"),
		value: []byte("developer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = collection.Insert(&Item{
		key:   []byte("anders"),
		value: []byte("manager"),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = collection.Insert(&Item{
		key:   []byte("martin"),
		value: []byte("developer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = collection.Insert(&Item{
		key:   []byte("ramya"),
		value: []byte("developer"),
	})
	if err != nil {
		t.Fatal(err)
	}
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
	if err := d.Deserialize(collection, 6); err != nil {
		t.Fatal(err)
	}
	banana, err := collection.Find([]byte("peter"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%s, %s\n", banana.key, banana.value)
}
