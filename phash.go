package main

import (
	"fmt"
	"os"
	"log"
	"bufio"
	"time"
)

func Recurse(tree *Tree, correct uint, wrong uint, bits *[64]bool, pos int, mutationCount int, results chan uint) {
	if correct != 0 {
		Find(tree, correct, bits, pos + 1, mutationCount, results)
	}

	if mutationCount > 0 && wrong != 0 {
		Find(tree, wrong, bits, pos + 1, mutationCount - 1, results)
	}
}

func Find(tree *Tree, node uint, bits *[64]bool, pos int, mutationCount int, results chan uint) {
	if tree.IsLeaf(node) {
		results <- tree.Value(node)
		return
	}

	on := tree.On(node)
	off := tree.Off(node)
	if (*bits)[pos] {
		Recurse(tree, on, off, bits, pos, mutationCount, results)
	} else {
		Recurse(tree, off, on, bits, pos, mutationCount, results)
	}
}

func bitsOf(number uint64) [64]bool {
	var result [64]bool
	for n := 0; n < 64; n++ {
		result[63 - n] = number & 1 == 1
		number >>= 1
	}

	return result
}

type Tree struct {
	nodes    []uint
	next     uint
	capacity uint
}

const NoChild uint = 0
const LeafMarker uint = 0xffffffff

func NewTree(capacity uint) *Tree {
	return &Tree{
		nodes: make([]uint, 2 * capacity),
		next: 1,
		capacity: capacity,
	}
}

func (tree *Tree) On(node uint) uint {
	return tree.nodes[2 * node]
}

func (tree *Tree) Off(node uint) uint {
	return tree.nodes[2 * node + 1]
}

func (tree *Tree) IsLeaf(node uint) bool {
	return tree.nodes[2 * node] == LeafMarker
}

func (tree *Tree) Value(node uint) uint {
	return tree.nodes[2 * node + 1]
}

func (tree *Tree) NewNode() (node uint) {
	node = tree.next
	tree.next++
	return
}

func (tree *Tree) Root() uint {
	return 0
}

func (tree *Tree) SetOn(parent uint, child uint) {
	tree.nodes[2 * parent] = child
}

func (tree *Tree) SetOff(parent uint, child uint) {
	tree.nodes[2 * parent + 1] = child
}

func (tree *Tree) SetLeaf(node uint, value uint) {
	tree.nodes[2 * node] = LeafMarker
	tree.nodes[2 * node + 1] = value
}

func addHashToTree(tree *Tree, item uint, hash uint64) {
	node := tree.Root()
	for _, on := range bitsOf(hash) {
		if on {
			if tree.On(node) == 0 {
				tree.SetOn(node, tree.NewNode())
			}

			node = tree.On(node)
		} else {
			if tree.Off(node) == 0 {
				tree.SetOff(node, tree.NewNode())
			}

			node = tree.Off(node)
		}
	}

	tree.SetLeaf(node, item)
}

type TestEntry struct {
	hash [64]bool
	item uint
}

func readTestSet(filename string) []TestEntry {
	var entries []TestEntry

	fp, err := os.Open(filename)
	if err != nil {
		log.Fatal("Could not open file at ", filename, err)
	}

	defer fp.Close()

	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {

		var id uint
		var hash uint64

		line := scanner.Text()
		n, _ := fmt.Sscanf(line, "%d %d\n", &id, &hash)
		if n == 2 && hash != 0 {
			entries = append(entries, TestEntry{bitsOf(hash), id})
		}
	}

	return entries
}

func readPhashTree(filename string) *Tree {
	fp, err := os.Open(filename)
	if err != nil {
		log.Fatal("Could not open file at ", filename, err)
	}

	defer fp.Close()

	count := 0
	tree := NewTree(45000000)

	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		var id uint
		var hash uint64

		line := scanner.Text()
		n, _ := fmt.Sscanf(line, "%d %d\n", &id, &hash)
		if n == 2 && hash != 0 {
			count++
			addHashToTree(tree, id, hash)
		}
	}

	log.Print("Number of items in memory: ", count)

	return tree
}

func main() {
	tree := readPhashTree("/tmp/phash.txt")

	log.Println("Number of nodes", tree.next)

	testSet := readTestSet("/tmp/random.txt")

	log.Print("Starting queries now")
	start := time.Now()

	for _, testEntry := range testSet {
		for i := 0; i < 10; i++ {
			testEntry.hash[5*i] = !testEntry.hash[5*i]
		}

		results := make(chan uint)
		go func() {
			Find(tree, tree.Root(), &testEntry.hash, 0, 12, results)
			close(results)
		}()

		matched := false
		for node := range results {
			if node == testEntry.item {
				matched = true
			}
		}

		if !matched {
			log.Panicln("No match found for ", testEntry)
		}
	}

	log.Println("Searching took (on average) ", (time.Since(start) / time.Duration(len(testSet))).String())
}
