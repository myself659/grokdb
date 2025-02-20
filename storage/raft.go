package storage

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// SetupRaft 配置 Raft
func (n *StorageNode) SetupRaft(raftDir string, raftAddr string) error {
	// 创建 Raft 配置
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(n.ID)

	// 设置存储（日志和快照）
	logStore, err := raftboltdb.NewBoltStore(fmt.Sprintf("%s/log-%s.db", raftDir, n.ID))
	if err != nil {
		return err
	}
	stableStore, err := raftboltdb.NewBoltStore(fmt.Sprintf("%s/stable-%s.db", raftDir, n.ID))
	if err != nil {
		return err
	}
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 1, os.Stderr)
	if err != nil {
		return err
	}

	// 设置网络传输
	transport, err := raft.NewTCPTransport(raftAddr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// 创建 Raft 实例
	fsm := &FSM{node: n} // 自定义 FSM，见下文
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}
	n.Raft = r

	// 如果是第一个节点，启动单节点集群
	if len(n.peers) == 0 {
		r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		})
	} else {
		// 加入现有集群（这里简化，实际需要 leader 地址）
		log.Printf("Node %s joining peers: %v", n.ID, n.peers)
	}

	return nil
}

// FSM 实现 Raft 的状态机
type FSM struct {
	node *StorageNode
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	entry := string(log.Data)
	var key, value string
	fmt.Sscanf(entry, "SET %s %s", &key, &value)
	f.node.Data[key] = value
	fmt.Printf("FSM applied: %s = %s", key, value)
	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) { return nil, nil }
func (f *FSM) Restore(io.ReadCloser) error         { return nil }
