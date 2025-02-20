package storage

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

// WAL 结构
type WAL struct {
	file   *os.File
	writer *bufio.Writer
	reader *bufio.Reader // 新增读取器
	mu     sync.Mutex
}

// NewWAL 创建 WAL
func NewWAL(filename string) *WAL {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open WAL file: %v", err)
	}
	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
		reader: bufio.NewReader(file),
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

// Replay 重放日志
func (w *WAL) Replay(applyFunc func(string, string)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 重置文件指针到开头
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return err
	}
	w.reader = bufio.NewReader(w.file)

	// 逐行读取并应用
	for {
		line, err := w.reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		var key, value string
		if _, err := fmt.Sscanf(strings.TrimSpace(line), "SET %s %s", &key, &value); err == nil {
			applyFunc(key, value)
		}
	}
	return nil
}

// Close 关闭 WAL
func (w *WAL) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writer.Flush()
	w.file.Close()
}
