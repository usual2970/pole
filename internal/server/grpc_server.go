package server

import (
	"math"
	"net"
	"pole/internal/pb"
	"pole/internal/poled"
	"pole/internal/util/log"
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type GrpcServer struct {
	nodeService *NodeService
	listener    net.Listener
	grpcServer  *grpc.Server
}

func NewGrpcServer(nodeService *NodeService, poleService *PoleService) (*GrpcServer, error) {

	// Make the gRPC options.
	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(math.MaxInt64),
		grpc.MaxSendMsgSize(math.MaxInt64),
		grpc.StreamInterceptor(
			grpcmiddleware.ChainStreamServer(),
		),
		grpc.UnaryInterceptor(
			grpcmiddleware.ChainUnaryServer(),
		),
		grpc.KeepaliveParams(
			keepalive.ServerParameters{
				//MaxConnectionIdle:     0,
				//MaxConnectionAge:      0,
				//MaxConnectionAgeGrace: 0,
				Time:    5 * time.Second,
				Timeout: 5 * time.Second,
			},
		),
	}

	grpcServer := grpc.NewServer(
		grpcOpts...,
	)

	pb.RegisterNodeServer(grpcServer, nodeService)
	pb.RegisterPoleServer(grpcServer, poleService)

	listener, err := net.Listen("tcp", poled.GetGrpcAddr())
	if err != nil {
		return nil, err
	}

	rs := &GrpcServer{
		nodeService: nodeService,
		listener:    listener,
		grpcServer:  grpcServer,
	}

	return rs, nil
}

func (s *GrpcServer) Start() error {

	go func() {
		err := s.grpcServer.Serve(s.listener)
		if err != nil {
			log.WithField("module", "grpcServer").Error(err)
		}
	}()

	return nil
}

func (s *GrpcServer) Stop() error {
	s.grpcServer.GracefulStop()
	return nil
}
