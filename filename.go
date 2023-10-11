package wal

import (
	"strconv"
	"strings"
)

type FileNameParser struct {
	name  string
	parts []string
}

func NewFileNameParser() *FileNameParser {
	return &FileNameParser{}
}

func (f *FileNameParser) Parse(name string) *FileNameParser {
	f.name = name
	f.parts = strings.Split(name, ".")
	return f
}

func (f *FileNameParser) FileName() string {
	return f.name
}

func (f *FileNameParser) Offset() (int, error) {
	return strconv.Atoi(f.parts[2])
}

func (f *FileNameParser) Segment() (int, error) {
	return strconv.Atoi(f.parts[1])
}

func (f *FileNameParser) SegmentAndOffset() (int, int, error) {
	segment, err := f.Segment()
	if err != nil {
		return 0, 0, err
	}
	offset, err := f.Offset()
	if err != nil {
		return 0, 0, err
	}
	return segment, offset, nil
}
