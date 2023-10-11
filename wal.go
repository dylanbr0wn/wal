package wal

import (
	"bufio"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Config struct {
	maxSegmentSize int
	root           string
	fileName       string
}

type WAL struct {
	segmentCount     int
	config           *Config
	currentSegmentId int
	writer           *bufio.Writer
	reader           *bufio.Reader
}

func New(config *Config) *WAL {
	wal := &WAL{
		segmentCount:     0,
		config:           config,
		currentSegmentId: 0,
	}

	if err := os.Mkdir(config.root, fs.ModePerm); err != nil {
		if os.IsExist(err) {
			info, err := os.Stat(config.root)
			if err != nil {
				panic(err)
			}
			if !info.IsDir() {
				panic("wal root is not a directory")
			}
		}
	}

	entries, err := os.ReadDir(config.root)
	if err != nil {
		panic(err)
	}
	if len(entries) == 0 {
		// need to create a new segment
	} else {
		// need to open the last segment
		logFiles, err := filepath.Glob(config.fileName + "*")
		if err != nil {
			panic(err)
		}
		sort.Strings(logFiles)
		lastLogFile := logFiles[len(logFiles)-1]

		file, err := os.OpenFile(lastLogFile, os.O_RDWR, fs.ModeAppend)
		if err != nil {
			panic(err)
		}
		wal.writer = bufio.NewWriter(file)
		wal.reader = bufio.NewReader(file)

		// file name anatomy: <file name>.<segment id>.<offset>.log

		split := strings.Split(lastLogFile, ".")
		if wal.currentSegmentId, err = strconv.Atoi(split[1]); err != nil {
			panic(err)
		}

		offset, err := strconv.Atoi(split[2])
		if err != nil {
			panic(err)
		}

		if _, err := file.Seek(int64(offset), 0); err != nil {
			panic(err)
		}

	}

}

func (w *WAL) Write(data []byte) error {
	return nil
}

func (w *WAL) Read() ([]byte, error) {
	return nil, nil
}

func (w *WAL) Close() error {
	return nil
}

func (w *WAL) Sync() error {
	return nil
}
