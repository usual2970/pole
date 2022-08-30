package client

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	//direct connect to service, it is can be used in k8s or other systems without service discovery
	DirectScheme    = "direct"
	DiscovScheme    = "discov"
	EndpointSepChar = ','
)

var (
	EndpointSep = fmt.Sprintf("%c", EndpointSepChar)
	Replica     = 2
)

func buildTarget(service string) string {
	return service
}

func New(service string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	target := buildTarget(service)
	conn, err := dial(target, append(defaultDialOpts, opts...)...)
	if err != nil {
		return nil, errors.Wrapf(err, "dial %s error\n", target)
	}
	return conn, nil
}

// Get new grpc client
func Get(service string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if val, ok := clients.Load(service); ok {
		if val.(*grpc.ClientConn).GetState() == connectivity.Ready {
			return val.(*grpc.ClientConn), nil
		}
	}

	mu.Lock()
	defer mu.Unlock()

	// 双检, 避免多次创建
	if val, ok := clients.Load(service); ok {
		if val.(*grpc.ClientConn).GetState() == connectivity.Ready {
			return val.(*grpc.ClientConn), nil
		}
	}

	conn, err := New(service, opts...)
	if err != nil {
		return nil, err
	}

	clients.Store(service, conn)
	return conn, nil
}

func dial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, target, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "DialContext error, target:%s\n", target)
	}
	return conn, nil
}
