package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_WAL(t *testing.T) {
	config := &Config{
		maxSegmentSize: 1024 * 1024,
		root:           "./wal",
		fileName:       "wal",
	}

	t.Run("Single Line", func(t *testing.T) {
		wal := New(config)
		wal.Reset()
		line := "hello world"
		if err := wal.Write([]byte(line)); err != nil {
			panic(err)
		}

		if err := wal.Sync(); err != nil {
			panic(err)
		}
		data, err := wal.Read(0)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, line, string(data[0]))
		wal.CloseAndRemove()
	})
	t.Run("Multiple Lines", func(t *testing.T) {
		wal := New(config)
		line := "hello world"

		for i := 0; i < 100; i++ {
			wal.Write([]byte(line))
		}
		if err := wal.Sync(); err != nil {
			panic(err)
		}

		offset := wal.Offset()
		assert.Equal(t, 100, offset)

		data, err := wal.Read(0)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, len(data), 100)
		for i := 0; i < 100; i++ {
			assert.Equal(t, line, string(data[i]))
		}
	})

}
