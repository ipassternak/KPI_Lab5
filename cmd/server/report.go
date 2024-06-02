package main

import (
	"encoding/json"
	"log"
	"net/http"
)

const reportMaxLen = 100

type Report struct {
	Requests []string `json:"requests"`
}

func (r *Report) Process(req *http.Request) {
	author := req.Header.Get("lb-author")
	log.Printf("GET some-data from [%s] request", author)
	if author == "" {
		author = "unknown"
	}
	for _, existingAuthor := range r.Requests {
		if existingAuthor == author {
			return
		}
	}
	if len(r.Requests) > reportMaxLen {
		r.Requests = r.Requests[1:]
	}
	r.Requests = append(r.Requests, author)
}

func (r *Report) ServeHTTP(rw http.ResponseWriter, _ *http.Request) {
	rw.Header().Set("content-type", "application/json")
	rw.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(rw).Encode(r)
}
