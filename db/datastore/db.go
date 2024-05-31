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

type hashEntry [2]int64
type hashIndex map[string]hashEntry

type Db struct {
	segment        *os.File
	segmentOffset  int64
	segmentIndex   int
	maxSegmentSize int64
	dir            string
	// writeChan      chan struct{key, value string}

	// Додаємо м'ютекс для безпечного доступу до hashIndex
	// mu             sync.Mutex

	index hashIndex
}

func NewDb(dir string, maxSegmentSize int64) (*Db, error) {
	db := &Db{
		index:          make(hashIndex),
		maxSegmentSize: maxSegmentSize,
		dir:            dir,
		// writeChan:      make(chan struct{key, value string}),
		// mu:             sync.Mutex{},
	}
	err := db.recover()
	if err != nil {
		return nil, err
	}

	// Ініціалізуємо нову рутину для запису в файл:
	// Рутина повинна містити нескінченний цикл, який чекає на дані в каналі та на стоп сигнал
	// for data := range writeChan {
	// 	// Записуємо дані в файл
	//	}
	// В канал повинна надходити структура, яка містить ключ та значення
	// {key: "key", value: "value"}
	// Якщо ми хочемо хендлити помилки, то можемо використати канал для помилок
	// Канал для помилок може бути реалізовний в межах Put та БД
	// Можна взяти код з Put
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
		value, err := db.Get(key)
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
	db.segment.Close()
	db.segmentIndex = 0
	db.index = index
	db.segmentOffset = segmentOffset
	db.segment = segment
	for i := 1; i <= segmentIndex; i++ {
		segmentPath := db.toSegmentPath(int64(i))
		os.Remove(segmentPath)
	}
	return nil
}
