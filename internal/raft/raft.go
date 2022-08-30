package raft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"pole/internal/pb"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
)

func NewRaft(myID, myAddress, raftDir, join string, bootstrap bool, fsm raft.FSM) (*raft.Raft, error) {
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

		if _, err := cc.Join(context.Background(), &pb.JoinRequest{Id: myID, BindAddress: myAddress}); err != nil {
			return nil, fmt.Errorf("raft.Raft.Join: %v", err)
		}
	}

	return r, nil
}