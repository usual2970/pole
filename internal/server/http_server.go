package server

import (
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
)

type HttpServer struct {
	address  string
	router   *gin.Engine
	listener net.Listener
}

func NewHttpServer(address string) (*HttpServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	router := gin.Default()
	router.Static("/admin", "./web/dist/spa")
	router.Static("/assets", "./web/dist/spa/assets")
	router.Static("/icons", "./web/dist/spa/icons")
	return &HttpServer{
		router:   router,
		address:  address,
		listener: listener,
	}, nil
}

func (s *HttpServer) Start() error {
	if err := http.Serve(s.listener, s.router); err != nil {
		return err
	}
	return nil
}

func (s *HttpServer) Stop() error {
	return s.listener.Close()
}
