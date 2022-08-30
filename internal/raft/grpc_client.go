package raft

import (
	"errors"
	"pole/internal/client"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

var clientSf singleflight.Group
var errGetGprcClient = errors.New("get grpc client failed")
var clients sync.Map

func init() {
	client.DefaultTimeout = 1500 * time.Millisecond
}

func GetClientConn(service string) (*grpc.ClientConn, error) {

	if val, ok := clients.Load(service); ok {
		if val.(*grpc.ClientConn).GetState() == connectivity.Ready {
			return val.(*grpc.ClientConn), nil
		}
	}

	key := "pole_grpc_" + service
	rs, err, _ := clientSf.Do(key, func() (interface{}, error) {
		return client.New(
			service,
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                0,
				Timeout:             0,
				PermitWithoutStream: false,
			}),
			grpc.WithInsecure(),
		)
	})

	if err != nil {
		return nil, err
	}

	if rs == nil {
		return nil, errGetGprcClient
	}

	if cc, ok := rs.(*grpc.ClientConn); ok {
		clients.Store(service, cc)
		return cc, nil
	}

	return nil, errGetGprcClient
}
