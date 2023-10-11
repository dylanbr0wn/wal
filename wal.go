package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

var (
	lengthBufferSize = 4
)

type Config struct {
	maxLogSize     int
	maxNumSegments int
	root           string
	fileName       string
	maxSyncSize    int
}

var DefaultConfig = &Config{
	maxLogSize:     1024 * 1024 * 5,
	maxNumSegments: 10,
	maxSyncSize:    1024,
}

func resolveConfig(config *Config) *Config {
	if config == nil {
		return DefaultConfig
	}
	if config.maxLogSize == 0 {
		config.maxLogSize = DefaultConfig.maxLogSize
	}
	if config.maxNumSegments == 0 {
		config.maxNumSegments = DefaultConfig.maxNumSegments
	}
	if config.maxSyncSize == 0 {
		config.maxSyncSize = DefaultConfig.maxSyncSize
	}
	return config
}

type WAL struct {
	segmentCount     int
	config           *Config
	currentSegmentId int
	writer           *bufio.Writer
	offset           int
	currentLogSize   int64
	file             *os.File
	lengthBuffer     []byte
	fileNamePrefix   string
	syncChan         chan struct{}
	writeChan        chan int64
}

func New(config *Config) *WAL {

	wal := &WAL{
		segmentCount:     0,
		config:           resolveConfig(config),
		currentSegmentId: 0,
		offset:           0,
		currentLogSize:   0,
		lengthBuffer:     make([]byte, lengthBufferSize),
		fileNamePrefix:   filepath.Join(config.root, config.fileName),
		syncChan:         make(chan struct{}),
		writeChan:        make(chan int64),
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
		file, err := os.OpenFile(wal.getFileName(0, 0), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}
		wal.file = file
		wal.writer = bufio.NewWriter(file)
		wal.currentLogSize = 0
		wal.segmentCount = 0
	} else {
		// need to open the last segment
		logFiles, err := wal.getLogFilePaths()
		if err != nil {
			panic(err)
		}
		lastLogFile := logFiles[len(logFiles)-1]

		file, err := os.OpenFile(lastLogFile, os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}

		filestat, err := file.Stat()
		if err != nil {
			panic(err)
		}

		parser := NewFileNameParser()
		currentSegment, offset, err := parser.Parse(lastLogFile).SegmentAndOffset()
		if err != nil {
			panic(err)
		}
		wal.currentSegmentId = currentSegment

		// seek end of file
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			panic(err)
		}
		reader := bufio.NewReader(file)

		n, err := wal.discard(0, math.MaxInt, reader)
		if err != io.EOF {
			panic(err)
		}
		wal.offset = offset + n
		wal.file = file
		file.Seek(0, io.SeekEnd)
		wal.writer = bufio.NewWriter(file)
		wal.segmentCount = len(logFiles) - 1
		wal.currentLogSize = filestat.Size()
	}
	go wal.SyncWorker()
	return wal
}

func (w *WAL) SyncWorker() {
	currentSyncSize := int64(0)
	timer := time.NewTimer(time.Second)
	for {
		select {
		case <-timer.C:
			if err := w.Sync(); err != nil {
				panic(err)
			}
		case n := <-w.writeChan:
			currentSyncSize += n
			if currentSyncSize >= int64(w.config.maxSyncSize) {
				if err := w.Sync(); err != nil {
					panic(err)
				}
				currentSyncSize = 0
			}
		case <-w.syncChan:
			currentSyncSize = 0
		}
	}
}

func (w *WAL) getFileName(segment int, offset int) string {
	return fmt.Sprintf("%s.%d.%d.log", w.fileNamePrefix, segment, offset)
}

func (w *WAL) discard(start int, end int, reader *bufio.Reader) (int, error) {
	current := start
	for current < end {
		var err error
		w.lengthBuffer, err = reader.Peek(lengthBufferSize)
		if err != nil {
			return 0, err
		}
		dataSize := binary.LittleEndian.Uint32(w.lengthBuffer)
		_, err = reader.Discard(lengthBufferSize + int(dataSize))
		if err != nil {
			return 0, err
		}
		current++
	}
	return current - start, nil
}

func (w *WAL) Write(data []byte) error {

	if w.currentLogSize+int64(len(data)) > int64(w.config.maxLogSize) {
		// flush and close current segment
		if err := w.Sync(); err != nil {
			return err
		}
		w.syncChan <- struct{}{}

		if err := w.file.Close(); err != nil {
			return err
		}

		// create new segment

		w.currentSegmentId++
		file, err := os.OpenFile(w.getFileName(w.currentSegmentId, w.offset), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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

		if w.segmentCount > w.config.maxNumSegments {
			// remove oldest segment
			logFiles, err := w.getLogFilePaths()
			if err != nil {
				return err
			}
			if err := os.Remove(logFiles[0]); err != nil {
				return err
			}
			w.segmentCount--
		}

	}

	buf := make([]byte, lengthBufferSize)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	if _, err := w.writer.Write(buf); err != nil {
		return err
	}
	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	amountWritten := int64(lengthBufferSize + len(data))
	w.writeChan <- amountWritten
	w.currentLogSize += amountWritten
	w.offset++

	return nil
}

func (w *WAL) getLogFilePaths() ([]string, error) {
	logFiles, err := filepath.Glob(w.fileNamePrefix + "*")
	if err != nil {
		return nil, err
	}
	sort.Strings(logFiles)
	return logFiles, nil
}

func getFileContainingOffset(files []string, offset int) (string, int, int, error) {
	logFileIndex := 0
	startOffset := 0
	fileNameParser := NewFileNameParser()
	for i, logFile := range files {
		fileStartOffset, err := fileNameParser.Parse(logFile).Segment()
		if err != nil {
			return "", 0, 0, err
		}
		if offset < fileStartOffset {
			break
		}
		logFileIndex = i
		startOffset = fileStartOffset
	}
	return files[logFileIndex], logFileIndex, startOffset, nil
}

func (w *WAL) Read(offset int) ([][]byte, error) {
	logFiles, err := w.getLogFilePaths()
	if err != nil {
		return nil, err
	}

	_, logFileIndex, startOffset, err := getFileContainingOffset(logFiles, offset)
	if err != nil {
		return nil, err
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
			if _, err = w.discard(startOffset, offset, &reader); err != nil {
				return nil, err
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
	if err := w.file.Close(); err != nil {
		return err
	}
	close(w.syncChan)
	close(w.writeChan)
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

// func (w *WAL) Reset() error {
// 	if err := w.file.Close(); err != nil {
// 		return err
// 	}
// 	if err := os.RemoveAll(w.config.root); err != nil {
// 		return err
// 	}
// 	if err := os.Mkdir(w.config.root, fs.ModePerm); err != nil {
// 		if os.IsExist(err) {
// 			info, err := os.Stat(w.config.root)
// 			if err != nil {
// 				panic(err)
// 			}
// 			if !info.IsDir() {
// 				panic("wal root is not a directory")
// 			}
// 		}
// 	}
// 	w.currentSegmentId = 0
// 	w.offset = 0
// 	file, err := os.OpenFile(w.getFileName(0, 0), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
// 	if err != nil {
// 		panic(err)
// 	}
// 	w.file = file
// 	w.writer = bufio.NewWriter(file)
// 	w.currentLogSize = 0
// 	w.segmentCount = 0
// 	return nil
// }

func (w *WAL) CloseAndRemove() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(w.config.root); err != nil {
		return err
	}
	return nil
}
