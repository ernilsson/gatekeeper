package dal

import (
	"bytes"
	"testing"
)

func TestSplit(t *testing.T) {
	provideItem := func(id string) *Item {
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
					provideItem("one"),
					provideItem("two"),
					provideItem("three"),
					provideItem("four"),
				},
			},
			expected: [2]*Node{
				{
					items: []*Item{
						provideItem("one"),
						provideItem("two"),
					},
				},
				{
					items: []*Item{
						provideItem("four"),
					},
				},
			},
		},
		{
			name: "given even number of items on parent node",
			node: &Node{
				items: []*Item{
					provideItem("one"),
					provideItem("two"),
					provideItem("three"),
					provideItem("four"),
				},
				children: []uint64{1, 2, 3, 4, 5},
			},
			expected: [2]*Node{
				{
					items: []*Item{
						provideItem("one"),
						provideItem("two"),
					},
					children: []uint64{1, 2},
				},
				{
					items: []*Item{
						provideItem("four"),
					},
					children: []uint64{3, 4, 5},
				},
			},
		},
		{
			name: "given odd number of items on leaf node",
			node: &Node{
				items: []*Item{
					provideItem("one"),
					provideItem("two"),
					provideItem("three"),
				},
			},
			expected: [2]*Node{
				{
					items: []*Item{
						provideItem("one"),
					},
				},
				{
					items: []*Item{
						provideItem("three"),
					},
				},
			},
		},
		{
			name: "given odd number of items on parent node",
			node: &Node{
				items: []*Item{
					provideItem("one"),
					provideItem("two"),
					provideItem("three"),
				},
				children: []uint64{1, 2, 3, 4},
			},
			expected: [2]*Node{
				{
					items: []*Item{
						provideItem("one"),
					},
					children: []uint64{1, 2},
				},
				{
					items: []*Item{
						provideItem("three"),
					},
					children: []uint64{3, 4},
				},
			},
		},
	}

	unequal := func(a, b *Node) bool {
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
			if unequal(m.expected[0], a) {
				t.Fatalf("got %v; want %v", a, m.expected[0])
			}
			if unequal(m.expected[1], b) {
				t.Fatalf("got %v; want %v", b, m.expected[1])
			}
		})
	}
}
