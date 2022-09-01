package raft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"pole/internal/conf"
	"pole/internal/pb"
	"pole/internal/poled/meta"
	"pole/internal/util/log"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
)

type raftNode struct {
	cancel context.CancelFunc
	Raft   *raft.Raft
	id     string
	meta   *meta.Meta
}

func NewRaft(ctx context.Context, myID, myAddress, raftDir, join string, bootstrap bool, fsm *meta.Meta) (*raftNode, error) {
	lg := log.WithField("module", "initRaft")
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

	baseDir := filepath.Join(raftDir, myID)

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	trans, err := raft.NewTCPTransport(myAddress, nil, 3, 5*time.Second, os.Stdout)
	if err != nil {
		return nil, fmt.Errorf(`raft.NewTCPTransport(%q, ...): %v`, baseDir, err)
	}

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, trans)
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	if bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(myID),
					Address:  raft.ServerAddress(myAddress),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
		}
	}

	if join != "" {
		client, err := GetClientConn(join)
		if err != nil {
			return nil, fmt.Errorf("raft.Raft.Join: %v", err)
		}
		cc := pb.NewNodeClient(client)

		if _, err := cc.Join(ctx, &pb.JoinRequest{Id: myID, BindAddress: myAddress}); err != nil {
			lg.Error("raft.Raft.Join: ", err)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	node := &raftNode{Raft: r, cancel: cancel, id: myID, meta: fsm}

	go func() {
		node.process(ctx)
	}()
	return node, nil
}

func (n *raftNode) process(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case isLearder := <-n.Raft.LeaderCh():
			if isLearder {
				cmd, _ := meta.NewBecomeLeaderCmd(conf.GetGrpcAddr())
				n.Raft.Apply(cmd, 1*time.Second)
			}
		}
	}
}

func (n *raftNode) Stop(ctx context.Context) error {
	if n.Raft.State() != raft.Leader {
		client, err := GetClientConn(n.meta.Leader())
		if err != nil {
			return fmt.Errorf("raft.Raft.GetClient: %v", err)
		}
		cc := pb.NewNodeClient(client)

		if _, err := cc.Leave(ctx, &pb.LeaveRequest{Id: n.id}); err != nil {
			return fmt.Errorf("raft.Raft.Leave: %v", err)
		}
	}

	n.cancel()
	return nil
}
