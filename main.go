package main

import (
	"fmt"
	"log"
	"time"

	"github.com/myself659/grokdb/storage"
)

func main() {
	// 创建 3 个节点
	nodes := make([]*storage.StorageNode, 3)
	ports := []string{"19001", "19002", "19003"}
	for i, port := range ports {
		peers := []string{}
		for j, p := range ports {
			if j != i {
				peers = append(peers, "localhost:"+p)
			}
		}
		nodes[i] = storage.NewStorageNode(fmt.Sprintf("node%d", i+1), peers)
		err := nodes[i].SetupRaft(fmt.Sprintf("raft%d", i+1), "localhost:"+port)
		if err != nil {
			log.Fatalf("Setup Raft failed for node%d: %v", i+1, err)
		}
	}

	// 等待 Raft 选举
	time.Sleep(5 * time.Second)

	// 写入数据
	err := nodes[0].Write("user_id", "12345")
	if err != nil {
		log.Fatalf("Write failed: %v", err)
	}

	// 模拟 node2 宕机后重启
	log.Println("Simulating node2 crash and restart...")
	nodes[1].WAL.Close() // 关闭原节点
	nodes[1] = storage.NewStorageNode("node2", []string{"localhost:9001", "localhost:9003"})
	nodes[1].SetupRaft("raft2", "localhost:9002")

	// 检查恢复结果
	time.Sleep(1 * time.Second)
	for i, node := range nodes {
		value, exists := node.Read("user_id")
		log.Printf("Node %d: user_id = %s, exists = %v", i+1, value, exists)
	}
}
