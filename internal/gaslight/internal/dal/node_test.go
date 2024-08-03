package dal

import (
	"bytes"
	"testing"
)

func TestSplit(t *testing.T) {
	item := func(id string) *Item {
		return &Item{
			key:   []byte("key_" + id),
			value: []byte("value_" + id),
		}
	}
	matrix := []struct {
		name     string
		node     *Node
		expected [2]*Node
	}{
		{
			name: "given even number of items on leaf node",
			node: &Node{
				items: []*Item{
					item("one"),
					item("two"),
					item("three"),
					item("four"),
				},
			},
			expected: [2]*Node{
				{
					items: []*Item{
						item("one"),
						item("two"),
					},
				},
				{
					items: []*Item{
						item("four"),
					},
				},
			},
		},
		{
			name: "given even number of items on parent node",
			node: &Node{
				items: []*Item{
					item("one"),
					item("two"),
					item("three"),
					item("four"),
				},
				children: []uint64{1, 2, 3, 4, 5},
			},
			expected: [2]*Node{
				{
					items: []*Item{
						item("one"),
						item("two"),
					},
					children: []uint64{1, 2, 3},
				},
				{
					items: []*Item{
						item("four"),
					},
					children: []uint64{4, 5},
				},
			},
		},
		{
			name: "given odd number of items on leaf node",
			node: &Node{
				items: []*Item{
					item("one"),
					item("two"),
					item("three"),
				},
			},
			expected: [2]*Node{
				{
					items: []*Item{
						item("one"),
					},
				},
				{
					items: []*Item{
						item("three"),
					},
				},
			},
		},
		{
			name: "given odd number of items on parent node",
			node: &Node{
				items: []*Item{
					item("one"),
					item("two"),
					item("three"),
				},
				children: []uint64{1, 2, 3, 4},
			},
			expected: [2]*Node{
				{
					items: []*Item{
						item("one"),
					},
					children: []uint64{1, 2},
				},
				{
					items: []*Item{
						item("three"),
					},
					children: []uint64{3, 4},
				},
			},
		},
	}

	different := func(a, b *Node) bool {
		for i := range a.items {
			if bytes.Compare(a.items[i].key, b.items[i].key) != 0 {
				return true
			}
			if bytes.Compare(a.items[i].value, b.items[i].value) != 0 {
				return true
			}
		}
		for i := range a.children {
			if a.children[i] != b.children[i] {
				return true
			}
		}
		return false
	}
	for _, m := range matrix {
		t.Run(m.name, func(t *testing.T) {
			a, b := Split(m.node)
			if different(m.expected[0], a) {
				t.Fatalf("got %v; want %v", a, m.expected[0])
			}
			if different(m.expected[1], b) {
				t.Fatalf("got %v; want %v", b, m.expected[1])
			}
		})
	}
}
