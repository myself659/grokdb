package storage

import (
	"bufio"
	"log"
	"os"
	"sync"
)

// WAL 实现简单的日志存储
type WAL struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
}

// NewWAL 创建 WAL
func NewWAL(filename string) *WAL {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open WAL file: %v", err)
	}
	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
	}
}

// Write 写入日志
func (w *WAL) Write(entry string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.writer.WriteString(entry + "\n")
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

// Close 关闭 WAL
func (w *WAL) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writer.Flush()
	w.file.Close()
}
