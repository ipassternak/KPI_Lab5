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
	SEGMENT_EXT = ".seg"
)

type hashIndex map[string][2]int64

type Db struct {
	out            *os.File
	outOffset      int64
	segmentIndex   int
	maxSegmentSize int64
	outDir         string
	// writeChan      chan struct{key, value string}

	// Додаємо м'ютекс для безпечного доступу до hashIndex
	// mu             sync.Mutex

	index hashIndex
}

// Дописати метод Close, який завершує роботу БД
// close(db.writeChan)
// Закрити файли для читання, виставити флаг isClosed
// При спробі запису/читання в закриту БД повертати помилку

func (db *Db) loadSegment() error {
	segmentPath := filepath.Join(db.outDir, fmt.Sprintf("%d.db", db.segmentIndex))
	segment, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	db.out = segment
	db.outOffset = 0
	return nil
}

func NewDb(dir string, maxSegmentSize int64) (*Db, error) {
	db := &Db{
		index:          make(hashIndex),
		maxSegmentSize: maxSegmentSize,
		outDir:         dir,
		// writeChan:      make(chan struct{key, value string}),
		// mu:             sync.Mutex{},
	}
	err := db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	err = db.loadSegment()
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

const bufSize = 8192

func (db *Db) recoverSegmentIndex() error {
	files, err := os.ReadDir(db.outDir)
	if err != nil {
		return err
	}

	var segmentIndex int

	for _, file := range files {
		filename := file.Name()
		if filepath.Ext(filename) == SEGMENT_EXT {
			basename := strings.TrimSuffix(filename, SEGMENT_EXT)
			index, err := strconv.Atoi(basename)
			if err != nil {
				return err
			}
			if index > segmentIndex {
				db.segmentIndex = index
			}
		}
	}

	return nil
}

func (db *Db) recover() error {
	err := db.recoverSegmentIndex()
	if err != nil {
		return err
	}
	for i := 0; i <= db.segmentIndex; i++ {
		input, err := os.OpenFile(fmt.Sprintf("%d%s", i, SEGMENT_EXT), os.O_RDONLY, 0o600)
		if err != nil {
			return err
		}
		defer input.Close()

		var buf [bufSize]byte
		in := bufio.NewReaderSize(input, bufSize)
		for err == nil {
			var (
				header, data []byte
				n            int
			)
			header, err = in.Peek(bufSize)
			if err == io.EOF {
				if len(header) == 0 {
					return err
				}
			} else if err != nil {
				return err
			}
			size := binary.LittleEndian.Uint32(header)

			if size < bufSize {
				data = buf[:size]
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
				db.index[e.key] = [2]int64{int64(i), db.outOffset}
				db.outOffset += int64(n)
			}
		}
	}
	return nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	// db.mu.Lock()
	segmentInfo, ok := db.index[key]
	// db.mu.Unlock()
	if !ok {
		return "", ErrNotFound
	}

	segmentIndex := segmentInfo[0]
	segmentOffset := segmentInfo[1]

	file, err := os.Open(fmt.Sprintf("%d%s", segmentIndex, SEGMENT_EXT))
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
	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[key] = [2]int64{int64(db.segmentIndex), db.outOffset}
		db.outOffset += int64(n)
		if db.outOffset >= db.maxSegmentSize {
			db.out.Close()
			db.segmentIndex++
			err := db.loadSegment()
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (db *Db) Merge() error {
	swapFilename := fmt.Sprintf("%d%s", time.Now().Unix(), SEGMENT_EXT)
	swapFile, err := os.OpenFile(swapFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		swapFile.Close()
		os.Remove(swapFilename)
	}()
	for key := range db.index {
		value, err := db.Get(key)
		if err != nil {
			return err
		}
		e := entry{
			key:   key,
			value: value,
		}
		_, err = swapFile.Write(e.Encode())
		if err != nil {
			return err
		}
	}

	for i := 0; i <= db.segmentIndex; i++ {
		segmentPath := filepath.Join(db.outDir, fmt.Sprintf("%d%s", i, SEGMENT_EXT))
		err := os.Remove(segmentPath)
		if err != nil {
			return err
		}
	}

	err = os.Rename(swapFilename, fmt.Sprintf("%d%s", 0, SEGMENT_EXT))
	if err != nil {
		return err
	}

	return nil
}
