package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFound = fmt.Errorf("record does not exist")
	ErrDbClosed = fmt.Errorf("db is closed")
)

const (
	DbSegmentExt      = ".seg"
	recoverbufferSize = 8192
)

type DbOptions struct {
	MaxSegmentSize int64
	WorkerPoolSize int
}

type hashEntry [2]int64
type hashIndex map[string]hashEntry

type writeMsg struct {
	e     entry
	errCh chan error
}

type Db struct {
	segment        *os.File
	segmentOffset  int64
	segmentIndex   int
	maxSegmentSize int64
	dir            string
	writeCh        chan writeMsg
	mu             sync.RWMutex
	isClosed       bool
	wq             *workerQueue

	index hashIndex
}

func NewDb(dir string, options DbOptions) (*Db, error) {
	db := &Db{
		index:          make(hashIndex),
		writeCh:        make(chan writeMsg),
		maxSegmentSize: options.MaxSegmentSize,
		dir:            dir,
	}
	db.wq = newWorkerQueue(db.get, options.WorkerPoolSize)
	err := db.recover()
	if err != nil {
		return nil, err
	}
	go db.write()
	return db, nil
}

func (db *Db) setIndex(key string) {
	db.index[key] = hashEntry{int64(db.segmentIndex), db.segmentOffset}
}

func (db *Db) getIndex(key string) (int64, int64, bool) {
	segmentInfo, ok := db.index[key]
	return segmentInfo[0], segmentInfo[1], ok
}

func (db *Db) getSegmentPath() string {
	return db.toSegmentPath(int64(db.segmentIndex))
}

func (db *Db) toSegmentPath(index int64) string {
	filename := fmt.Sprintf("%d%s", index, DbSegmentExt)
	return filepath.Join(db.dir, filename)
}

func (db *Db) loadSegment() error {
	segmentPath := db.getSegmentPath()
	segment, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err == nil {
		db.segment = segment
		db.segmentOffset = 0
	}
	return err
}

func (db *Db) recoverSegmentIndex() (int, error) {
	files, err := os.ReadDir(db.dir)
	if err != nil {
		return -1, err
	}
	segmentIndex := -1
	for _, file := range files {
		filename := file.Name()
		if filepath.Ext(filename) == DbSegmentExt {
			basename := strings.TrimSuffix(filename, DbSegmentExt)
			index, err := strconv.Atoi(basename)
			if err != nil {
				return -1, err
			}
			if index > segmentIndex {
				segmentIndex = index
			}
		}
	}
	return segmentIndex, nil
}

func (db *Db) recover() error {
	segmentIndex, err := db.recoverSegmentIndex()
	if err != nil {
		return err
	}
	for i := 0; i <= segmentIndex; i++ {
		db.segmentIndex = i
		db.segmentOffset = 0
		segmentPath := db.getSegmentPath()
		input, err := os.OpenFile(segmentPath, os.O_RDONLY, 0o600)
		if err != nil {
			return err
		}
		defer input.Close()
		var buffer [recoverbufferSize]byte
		in := bufio.NewReaderSize(input, recoverbufferSize)
		for err == nil {
			var (
				header, data []byte
				n            int
			)
			header, err = in.Peek(recoverbufferSize)
			if err == io.EOF {
				if len(header) == 0 {
					continue
				}
			} else if err != nil {
				return err
			}
			size := binary.LittleEndian.Uint32(header)
			if size < recoverbufferSize {
				data = buffer[:size]
			} else {
				data = make([]byte, size)
			}
			n, err = in.Read(data)
			if err == nil {
				if n != int(size) {
					return fmt.Errorf("corrupted file")
				}
				var e entry
				e.Decode(data)
				db.setIndex(e.key)
				db.segmentOffset += int64(n)
			}
		}
	}
	return db.loadSegment()
}

func (db *Db) Close() error {
	if db.isClosed {
		return nil
	}
	close(db.writeCh)
	db.wq.Close()
	db.isClosed = true
	return db.segment.Close()
}

func (db *Db) get(key string) (string, error) {
	if db.isClosed {
		return "", ErrDbClosed
	}
	db.mu.RLock()
	segmentIndex, segmentOffset, found := db.getIndex(key)
	db.mu.RUnlock()
	if !found {
		return "", ErrNotFound
	}
	segmentPath := db.toSegmentPath(segmentIndex)
	file, err := os.Open(segmentPath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	_, err = file.Seek(segmentOffset, 0)
	if err != nil {
		return "", err
	}
	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) Get(key string) (string, error) {
	return db.wq.Do(key)
}

func (db *Db) write() {
	for msg := range db.writeCh {
		db.mu.Lock()
		n, err := db.segment.Write(msg.e.Encode())
		if err != nil {
			msg.errCh <- fmt.Errorf("failed to put %s: %s", msg.e.key, msg.e.value)
		} else {
			msg.errCh <- nil
			db.setIndex(msg.e.key)
			db.segmentOffset += int64(n)
			if db.segmentOffset >= db.maxSegmentSize {
				db.segment.Close()
				db.segmentIndex++
				db.loadSegment()
			}
		}
		db.mu.Unlock()
	}
}

func (db *Db) Put(key, value string) error {
	if db.isClosed {
		return ErrDbClosed
	}
	e := entry{
		key:   key,
		value: value,
	}
	errCh := make(chan error)
	db.writeCh <- writeMsg{e, errCh}
	return <-errCh
}

func (db *Db) Copy(filename string) (int64, hashIndex, error) {
	var (
		segmentOffset int64
		index         = make(hashIndex)
	)
	swap, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return 0, nil, err
	}
	defer swap.Close()
	for key := range db.index {
		value, err := db.get(key)
		if err != nil {
			os.Remove(filename)
			return 0, nil, err
		}
		e := entry{
			key:   key,
			value: value,
		}
		offset, err := swap.Write(e.Encode())
		if err != nil {
			os.Remove(filename)
			return 0, nil, err
		}
		index[key] = hashEntry{0, segmentOffset}
		segmentOffset += int64(offset)
	}
	return segmentOffset, index, nil
}

func (db *Db) Merge() error {
	if db.isClosed {
		return ErrDbClosed
	}
	swapFilename := db.toSegmentPath(time.Now().Unix())
	segmentOffset, index, err := db.Copy(swapFilename)
	if err != nil {
		return err
	}
	segmentIndex := db.segmentIndex
	segmentPath := db.toSegmentPath(0)
	err = os.Rename(swapFilename, segmentPath)
	if err != nil {
		return err
	}
	segment, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	db.mu.Lock()
	db.segment.Close()
	db.segmentIndex = 0
	db.index = index
	db.segmentOffset = segmentOffset
	db.segment = segment
	db.mu.Unlock()
	for i := 1; i <= segmentIndex; i++ {
		segmentPath := db.toSegmentPath(int64(i))
		os.Remove(segmentPath)
	}
	return nil
}
