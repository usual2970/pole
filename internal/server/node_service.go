package server

import (
	"context"
	"pole/internal/pb"

	"github.com/hashicorp/raft"
)

type NodeService struct {
	pb.UnimplementedNodeServer
	raft *raft.Raft
}

func NewNodeService(raft *raft.Raft) *NodeService {
	return &NodeService{
		raft: raft,
	}
}

func (s *NodeService) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	cf := s.raft.GetConfiguration()

	if err := cf.Error(); err != nil {

		return nil, err
	}

	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(req.Id) {
			return &pb.JoinResponse{Message: "success"}, nil
		}
	}

	f := s.raft.AddVoter(raft.ServerID(req.Id), raft.ServerAddress(req.BindAddress), 0, 0)
	if err := f.Error(); err != nil {
		return nil, err
	}
	return &pb.JoinResponse{Message: "success"}, nil
}

func (s *NodeService) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {

	cf := s.raft.GetConfiguration()

	if err := cf.Error(); err != nil {
		return nil, err
	}

	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(req.Id) {
			f := s.raft.RemoveServer(server.ID, 0, 0)
			if err := f.Error(); err != nil {
				return nil, err
			}

			return &pb.LeaveResponse{Message: "success"}, nil
		}
	}

	return &pb.LeaveResponse{Message: "success"}, nil
}
