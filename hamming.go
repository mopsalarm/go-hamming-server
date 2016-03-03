package main

import (
	"fmt"
	"os"
	"log"
	"bufio"
	"strconv"
	"sync"
	"net/http"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/steakknife/hamming"
	"github.com/gorilla/handlers"
)

type HashEntry struct {
	ItemId uint `json:"itemId"`
	Hash   uint64 `json:"hash"`
}

func load(filename string) (items []HashEntry, err error) {
	fp, err := os.Open(filename)
	if err != nil {
		return
	}

	defer fp.Close()

	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		line := scanner.Text()

		var id uint
		var hash uint64
		_, err = fmt.Sscanf(line, "%d %d", &id, &hash)
		if err != nil {
			return
		}

		if hash != 0 {
			items = append(items, HashEntry{id, hash})
		}
	}

	return
}

type SearchResult struct {
	Distance int `json:"distance"`
	ItemId   uint `json:"itemId"`
}

func linearSearch(entries []HashEntry, hash uint64) (results []SearchResult) {
	for _, entry := range entries {
		distance := hamming.Uint64(hash, entry.Hash)
		if distance <= 12 {
			results = append(results, SearchResult{distance, entry.ItemId})
		}
	}

	return
}

type Hashes struct {
	entries []HashEntry
	mutex   sync.RWMutex
}

func NewHashes(entries []HashEntry) *Hashes {
	return &Hashes{
		entries: entries,
		mutex: sync.RWMutex{},
	}
}

func (h *Hashes) similarTo(hash uint64) []SearchResult {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	return linearSearch(h.entries, hash)
}

func (h *Hashes) add(itemId uint, hash uint64) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.entries = append(h.entries, HashEntry{itemId, hash})
}

func main() {
	entries, err := load("phash.txt")
	if err != nil {
		log.Println("Could not read entries file, error:", err)
	}

	log.Println("Number of hashes loaded: ", len(entries))

	hashes := NewHashes(entries)

	router := mux.NewRouter()
	router.Path("/{hash}/similar").Methods("GET").HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		hash, err := strconv.ParseUint(vars["hash"], 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		results := hashes.similarTo(hash)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	router.Path("/{hash}").Methods("PUT").HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		hash, err := strconv.ParseUint(vars["hash"], 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var itemId uint
		if json.NewDecoder(req.Body).Decode(&itemId) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		hashes.add(itemId, hash)
	})

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", 8080),
		handlers.LoggingHandler(os.Stdout,
			handlers.RecoveryHandler()(router))))
}
