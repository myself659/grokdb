package storage

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// StorageNode 表示存储节点
type StorageNode struct {
	ID    string
	Data  map[string]string // 数据存储（内存中，实际可用磁盘）
	WAL   *WAL              // Write-Ahead Logging
	Raft  *raft.Raft        // Raft 实例
	mu    sync.Mutex
	peers []string // 其他节点地址
}

// NewStorageNode 创建并初始化节点
func NewStorageNode(id string, peers []string) *StorageNode {
	node := &StorageNode{
		ID:    id,
		Data:  make(map[string]string),
		WAL:   NewWAL(fmt.Sprintf("wal-%s.log", id)),
		peers: peers,
	}

	// 重放 WAL 恢复状态
	err := node.RestoreFromWAL()
	if err != nil {
		log.Printf("Failed to restore from WAL for node %s: %v", id, err)
	}
	return node
}

// RestoreFromWAL 重放 WAL 日志恢复数据
func (n *StorageNode) RestoreFromWAL() error {
	return n.WAL.Replay(func(key, value string) {
		n.mu.Lock()
		n.Data[key] = value
		n.mu.Unlock()
		log.Printf("Restored: %s = %s on node %s", key, value, n.ID)
	})
}

// Write 写入数据
func (n *StorageNode) Write(key, value string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	logEntry := fmt.Sprintf("SET %s %s", key, value)
	if err := n.WAL.Write(logEntry); err != nil {
		return err
	}

	if n.Raft != nil && n.Raft.State() == raft.Leader {
		future := n.Raft.Apply([]byte(logEntry), 5*time.Second)
		if err := future.Error(); err != nil {
			return err
		}
	}

	n.Data[key] = value
	log.Printf("Node %s: %s = %s", n.ID, key, value)
	return nil
}

// Read 读取数据
func (n *StorageNode) Read(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	value, exists := n.Data[key]
	return value, exists
}
