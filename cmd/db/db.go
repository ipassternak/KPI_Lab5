package main

import (
	"encoding/json"
	"net/http"

	datastore "github.com/roman-mazur/architecture-practice-4-template/db/datastore"
)

const (
	dir         = ".db"
	segmentSize = 10 * 1024 * 1024 // 10MB
	poolSize    = 1000
)

type Result struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	db, err := datastore.NewDb(dir, datastore.DbOptions{
		MaxSegmentSize: segmentSize,
		WorkerPoolSize: poolSize,
	})
	if err != nil {
		panic(err)
	}

	http.HandleFunc("GET /db/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		value, err := db.Get(key)
		switch err {
		case datastore.ErrNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case nil:
			break
		default:
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(Result{
			Key:   key,
			Value: value,
		})
	})

	http.HandleFunc("POST /db/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		var result Result
		if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		if err := db.Put(key, result.Value); err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	http.ListenAndServe(":5432", nil)
}
