package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// StorageNode 表示存储节点
type StorageNode struct {
	ID       string
	Data     map[string]string // 模拟存储
	Raft     *raft.Raft        // 分布式一致性
	mu       sync.Mutex
}

// NewStorageNode 创建存储节点
func NewStorageNode(id string) *StorageNode {
	return &StorageNode{
		ID:   id,
		Data: make(map[string]string),
	}
}

// WriteLog 写入日志并复制
func (n *StorageNode) WriteLog(key, value string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 模拟日志写入
	logEntry := fmt.Sprintf("SET %s %s", key, value)
	if n.Raft != nil {
		// 通过 Raft 复制到其他节点
		future := n.Raft.Apply([]byte(logEntry), 5*time.Second)
		if err := future.Error(); err != nil {
			return err
		}
	}

	// 本地存储
	n.Data[key] = value
	log.Printf("Node %s: %s = %s", n.ID, key, value)
	return nil
}

func main() {
	node := NewStorageNode("node1")
	// 这里省略 Raft 初始化，实际需要配置多节点
	err := node.WriteLog("user_id", "12345")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(node.Data)
}