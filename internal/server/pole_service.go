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
