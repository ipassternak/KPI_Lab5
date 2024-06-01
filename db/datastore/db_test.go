package datastore

import (
	"os"
	"sync"
	"testing"
)

const segmentSize = 1024
const poolSize = 1000

func TestDb_Put(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, DbOptions{
		MaxSegmentSize: segmentSize,
		WorkerPoolSize: poolSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	outFile, err := os.Open(db.getSegmentPath())
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	pairs = append(pairs, []string{"key4", "value4"})

	t.Run("segmentation", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			err := db.Put("key4", "value4")
			if err != nil {
				t.Fatal(err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if outInfo.Size() < segmentSize {
			t.Errorf("Unexpected size (%d)", outInfo.Size())
		}
		segments, _ := os.ReadDir(dir)
		if len(segments) < 2 {
			t.Errorf("Expected 2 or more segment files, got (%d)", len(segments))
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, DbOptions{
			MaxSegmentSize: segmentSize,
			WorkerPoolSize: poolSize,
		})
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("merge", func(t *testing.T) {
		err = db.Merge()
		if err != nil {
			t.Fatal(err)
		}
		segments, _ := os.ReadDir(dir)
		if len(segments) > 1 {
			t.Errorf("Expected single segment file after merge, got (%d)", len(segments))
		}
		for _, pair := range pairs {
			value, _ := db.Get(pair[0])
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
		err = db.Put("key5", "value5")
		if err != nil {
			t.Errorf("Cannot put %s: %s", "key5", err)
		}
		value, _ := db.Get("key5")
		if value != "value5" {
			t.Errorf("Bad value returned expected %s, got %s", "value5", value)
		}
	})

	t.Run("parallel reading", func(t *testing.T) {
		var (
			w   sync.WaitGroup
			err error
		)
		w.Add(poolSize * 5)
		for range poolSize * 5 {
			go func() {
				defer w.Done()
				_, err = db.Get("key2")
			}()
		}
		w.Wait()
		if err != nil {
			t.Fatal(err)
		}
	})
}
