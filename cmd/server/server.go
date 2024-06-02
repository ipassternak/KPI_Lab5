package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var (
	port     = flag.Int("port", 8080, "server port")
	teamName = "breaking_code"
	url      = "http://db:5432/db"
	body     = fmt.Sprintf(`{"value":"%s"}`, time.Now().Format("2006-01-02"))
)

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
	h := new(http.ServeMux)

	req, err := http.NewRequest(
		"POST",
		fmt.Sprintf("%s/%s", url, teamName),
		bytes.NewBuffer([]byte(body)),
	)

	if err != nil {
		panic(err)
	}

	_, err = http.DefaultClient.Do(req)

	if err != nil {
		panic(err)
	}

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := new(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		key := r.URL.Query().Get("key")
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", url, key), nil)

		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		res, err := http.DefaultClient.Do(req)

		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		if res.StatusCode == http.StatusNotFound {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_, _ = io.Copy(rw, res.Body)
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
