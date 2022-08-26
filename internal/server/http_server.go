package server

import (
	"errors"
	"net"
	"net/http"

	"pole/internal/poled"

	"github.com/gin-gonic/gin"
)

type HttpServer struct {
	address  string
	router   *gin.Engine
	listener net.Listener

	poled *poled.Poled
}

func NewHttpServer(address string, poled *poled.Poled) (*HttpServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	s := &HttpServer{

		address:  address,
		listener: listener,
		poled:    poled,
	}
	router := gin.Default()
	router.Static("/admin", "./web/dist/spa")
	router.Static("/assets", "./web/dist/spa/assets")
	router.Static("/icons", "./web/dist/spa/icons")

	router.GET("/_sql", s.exec)
	router.POST("/_sql", s.exec)
	s.router = router
	return s, nil
}

type SqlReq struct {
	Query string `form:"query" binding:"required"`
}

type BadRequestResp struct {
	Error string `json:"error"`
}

var (
	ErrBadRequest = errors.New("bad request")
)

func (s *HttpServer) exec(ctx *gin.Context) {
	param := &SqlReq{}
	if err := ctx.ShouldBind(param); err != nil {
		ctx.JSON(http.StatusBadRequest, &BadRequestResp{Error: ErrBadRequest.Error()})
		return
	}

	rs := s.poled.Exec(param.Query)
	ctx.JSON(rs.Code(), rs.Resp())
}

func (s *HttpServer) Start() error {
	go func() {
		_ = http.Serve(s.listener, s.router)
	}()
	return nil
}

func (s *HttpServer) Stop() error {
	return s.listener.Close()
}
