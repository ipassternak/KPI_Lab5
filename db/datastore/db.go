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
	"time"
)

var ErrNotFound = fmt.Errorf("record does not exist")

const (
	DbSegmentExt      = ".seg"
	recoverbufferSize = 8192
)

type hashIndex map[string][2]int64

type Db struct {
	segment        *os.File
	segmentOffset  int64
	segmentIndex   int
	maxSegmentSize int64
	dir            string

	index hashIndex
}

func NewDb(dir string, maxSegmentSize int64) (*Db, error) {
	db := &Db{
		index:          make(hashIndex),
		maxSegmentSize: maxSegmentSize,
		dir:            dir,
	}
	err := db.recover()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *Db) setIndex(key string) {
	db.index[key] = [2]int64{int64(db.segmentIndex), db.segmentOffset}
}

func (db *Db) getIndex(key string) (int64, int64, bool) {
	segmentInfo, ok := db.index[key]
	return segmentInfo[0], segmentInfo[1], ok
}

func (db *Db) getSegmentPath() string {
	filename := fmt.Sprintf("%d%s", db.segmentIndex, DbSegmentExt)
	return filepath.Join(db.dir, filename)
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
	return db.segment.Close()
}

func (db *Db) Get(key string) (string, error) {
	segmentIndex, segmentOffset, found := db.getIndex(key)
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

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	n, err := db.segment.Write(e.Encode())
	if err == nil {
		db.setIndex(key)
		db.segmentOffset += int64(n)
		if db.segmentOffset >= db.maxSegmentSize {
			db.segment.Close()
			db.segmentIndex++
			err = db.loadSegment()
		}
	}
	return err
}

func (db *Db) clearSegments() error {
	var err error
	db.segment.Close()
	for i := 0; i <= db.segmentIndex; i++ {
		segmentPath := db.toSegmentPath(int64(i))
		err = os.Remove(segmentPath)
	}
	if err != nil {
		segmentPath := db.getSegmentPath()
		db.segment, err = os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	}
	return err
}

func (db *Db) Merge() error {
	var (
		err    error
		offset int64
	)
	swapFilename := fmt.Sprintf("%d%s", time.Now().Unix(), DbSegmentExt)
	swap, err := os.OpenFile(swapFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		swap.Close()
		if err != nil {
			os.Remove(swapFilename)
		} else {
			db.segment = swap
			db.segmentOffset = int64(offset)
			db.segmentIndex = 0
		}
	}()
	for key := range db.index {
		var (
			value string
			n     int
		)
		value, err = db.Get(key)
		if err != nil {
			return err
		}
		e := entry{
			key:   key,
			value: value,
		}
		n, err = swap.Write(e.Encode())
		if err != nil {
			return err
		}
		offset += int64(n)
	}
	err = db.clearSegments()
	if err == nil {
		segmentPath := db.toSegmentPath(0)
		err = os.Rename(swapFilename, segmentPath)
	}
	return err
}
