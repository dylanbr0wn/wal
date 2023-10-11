package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var (
	lengthBufferSize = 4
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
	offset           int
	currentLogSize   int64
	file             *os.File
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
		wal.currentSegmentId = 0
		wal.offset = 0
		newLogName := fmt.Sprintf("%s.%d.%d.log", wal.config.fileName, wal.currentSegmentId, wal.offset)
		file, err := os.OpenFile(filepath.Join(wal.config.root, newLogName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}
		wal.file = file
		wal.writer = bufio.NewWriter(file)
		wal.currentLogSize = 0
		wal.segmentCount = 0
		wal.reader = bufio.NewReader(file)
	} else {
		// need to open the last segment
		logFiles, err := filepath.Glob(filepath.Join(config.root, config.fileName) + "*")
		if err != nil {
			panic(err)
		}
		sort.Strings(logFiles)
		lastLogFile := logFiles[len(logFiles)-1]

		file, err := os.OpenFile(lastLogFile, os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}

		filestat, err := file.Stat()
		if err != nil {
			panic(err)
		}
		// file name anatomy: <file name>.<segment id>.<offset>.log

		split := strings.Split(lastLogFile, ".")
		if wal.currentSegmentId, err = strconv.Atoi(split[1]); err != nil {
			panic(err)
		}

		offset, err := strconv.Atoi(split[2])
		if err != nil {
			panic(err)
		}

		// seek end of file
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			panic(err)
		}
		reader := bufio.NewReader(file)
		n := 0
		var readBytes []byte
		for {
			readBytes, err = reader.Peek(lengthBufferSize)
			if err != nil {
				break
			}
			dataSize := binary.LittleEndian.Uint32(readBytes)
			_, err = reader.Discard(lengthBufferSize + int(dataSize))
			if err != nil {
				break
			}
			n++
		}
		if err != io.EOF {
			panic(err)
		}
		wal.offset = offset + n
		wal.reader = reader
		wal.file = file
		file.Seek(0, io.SeekEnd)
		wal.writer = bufio.NewWriter(file)
		wal.segmentCount = len(logFiles) - 1
		wal.currentLogSize = filestat.Size()
	}
	return wal
}

func (w *WAL) Write(data []byte) error {

	if w.currentLogSize+int64(len(data)) > int64(w.config.maxSegmentSize) {
		// flush and close current segment
		if err := w.Sync(); err != nil {
			return err
		}

		if err := w.file.Close(); err != nil {
			return err
		}

		// create new segment

		w.currentSegmentId++
		newLogName := fmt.Sprintf("%s.%d.%d.log", w.config.fileName, w.currentSegmentId, w.offset)
		file, err := os.OpenFile(filepath.Join(w.config.root, newLogName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		_, err = file.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		w.file = file
		w.writer = bufio.NewWriter(file)
		w.currentLogSize = 0
		w.segmentCount++
	}

	buf := make([]byte, lengthBufferSize)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	if _, err := w.writer.Write(buf); err != nil {
		return err
	}
	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	w.currentLogSize += int64(len(data) + lengthBufferSize)
	w.offset++

	return nil
}

func (w *WAL) Read(offset int) ([][]byte, error) {
	logFiles, err := filepath.Glob(filepath.Join(w.config.root, w.config.fileName) + "*")
	if err != nil {
		return nil, err
	}
	println(len(logFiles))
	sort.Strings(logFiles)

	logFileIndex := 0
	startOffset := 0
	for i, logFile := range logFiles {
		parts := strings.Split(logFile, ".")
		fileStartOffset, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, err
		}
		if offset < fileStartOffset {
			break
		}
		logFileIndex = i
		startOffset = fileStartOffset
	}

	if logFileIndex < 0 {
		return nil, nil
	}

	var logData [][]byte
	var reader bufio.Reader
	for i, logFile := range logFiles[logFileIndex:] {
		file, err := os.Open(logFile)
		if err != nil {
			return nil, err
		}
		n := 0
		var readBytes []byte
		reader.Reset(file)
		if i == 0 {
			// seek to offset
			println("here")
			for startOffset < offset {
				readBytes, err = reader.Peek(lengthBufferSize)
				if err != nil {
					break
				}
				dataSize := binary.LittleEndian.Uint32(readBytes)
				_, err = reader.Discard(lengthBufferSize + int(dataSize))
				if err != nil {
					break
				}
				startOffset++
			}
		}
		for {
			readBytes, err = reader.Peek(lengthBufferSize)
			if err != nil {
				break
			}
			dataSize := binary.LittleEndian.Uint32(readBytes)
			_, err = reader.Discard(lengthBufferSize)
			if err != nil {
				break
			}
			data := make([]byte, dataSize)
			_, err = reader.Read(data)
			if err != nil {
				break
			}
			logData = append(logData, data)
			n++
		}
		if err != io.EOF {
			return nil, err
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
	}

	return logData, nil
}

func (w *WAL) Close() error {
	return nil
}

func (w *WAL) Sync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *WAL) Offset() int {
	return w.offset
}

func (w *WAL) Reset() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(w.config.root); err != nil {
		return err
	}
	if err := os.Mkdir(w.config.root, fs.ModePerm); err != nil {
		if os.IsExist(err) {
			info, err := os.Stat(w.config.root)
			if err != nil {
				panic(err)
			}
			if !info.IsDir() {
				panic("wal root is not a directory")
			}
		}
	}
	w.currentSegmentId = 0
	w.offset = 0
	newLogName := fmt.Sprintf("%s.%d.%d.log", w.config.fileName, w.currentSegmentId, w.offset)
	file, err := os.OpenFile(filepath.Join(w.config.root, newLogName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	w.file = file
	w.writer = bufio.NewWriter(file)
	w.currentLogSize = 0
	w.segmentCount = 0

	w.reader = bufio.NewReader(file)
	return nil
}

func (w *WAL) CloseAndRemove() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(w.config.root); err != nil {
		return err
	}
	return nil
}
