package server

import (
	"context"
	"pole/internal/pb"
	"pole/internal/poled"
)

type PoleService struct {
	pb.UnimplementedPoleServer
	poled *poled.Poled
}

func NewPoleService(poled *poled.Poled) *PoleService {
	return &PoleService{
		poled: poled,
	}
}

func (s *PoleService) Exec(ctx context.Context, req *pb.ExecRequest) (*pb.ExecResponse, error) {
	rs := s.poled.Exec(req.Sql)
	if err := rs.Error(); err != nil {
		return nil, err
	}
	return &pb.ExecResponse{Message: "success"}, nil
}

func (s *PoleService) Lock(ctx context.Context, req *pb.LockRequest) (*pb.LockResponse, error) {
	if err := s.poled.Lock(); err != nil {
		return nil, err
	}
	return &pb.LockResponse{Message: "success"}, nil
}

func (s *PoleService) Unlock(ctx context.Context, req *pb.UnlockRequest) (*pb.UnlockResponse, error) {
	if err := s.poled.Unlock(); err != nil {
		return nil, err
	}
	return &pb.UnlockResponse{Message: "success"}, nil
}
