package server

import (
	"errors"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
	"pole/internal/poled"
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

var (
	ErrBadRequest = errors.New("bad request")
)

func (s *HttpServer) exec(ctx *gin.Context) {
	param := &SqlReq{}
	if err := ctx.ShouldBind(param); err != nil {
		ctx.JSON(http.StatusBadRequest, ErrBadRequest.Error())
		return
	}

	rs := s.poled.Exec(param.Query)
	if rs.Error() != nil {
		ctx.JSON(http.StatusInternalServerError, rs.Error().Error())
		return
	}
	ctx.JSON(http.StatusOK, rs.Data())
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
