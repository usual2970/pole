package poled

import (
	"errors"
	"net/http"
)

type result interface {
	Error() error
	Resp() interface{}
	Code() int
}

var (
	ErrIndexExist         = errors.New("index already exists")
	ErrIndexNotFound      = errors.New("index not found")
	ErrWriterNotFound     = errors.New("writer not found")
	ErrReaderNotFound     = errors.New("reader not found")
	ErrSyntaxNotSupported = errors.New("syntax not supported")
	ErrBatchFailed        = errors.New("batch failed")
)

type generalResp struct {
	Error string `json:"error,omitempty"`
}

type generalResult struct {
	err error
}

func newGeneralResult(err error) *generalResult {
	return &generalResult{
		err: err,
	}
}

func (r *generalResult) Error() error {
	return r.err
}

func (r *generalResult) Resp() interface{} {
	rs := ""
	if r.err != nil {
		rs = r.err.Error()
	}
	return &generalResp{
		Error: rs,
	}
}

func (r *generalResult) Code() int {
	if r.Error() != nil {
		return http.StatusInternalServerError
	}
	return http.StatusOK
}

type selectResp struct {
	Took     int64 `json:"took"`
	TimedOut bool  `json:"timed_out"`
	Hits     Hits  `json:"hits"`
}

type Hits struct {
	Total    int64   `json:"total"`
	MaxScore float64 `json:"max_score"`
	Hits     []Hit   `json:"hits"`
}

type Hit struct {
	Index   string                 `json:"_index"`
	Type    string                 `json:"_type"`
	ID      string                 `json:"_id"`
	Score   float64                `json:"_score"`
	Routing string                 `json:"_routing"`
	Ignored []string               `json:"_ignored"`
	Source  map[string]interface{} `json:"_source"`
}

func (r *selectResp) Error() error {
	return nil
}

func (r *selectResp) Resp() interface{} {
	return r
}

func (r *selectResp) Code() int {
	return http.StatusOK
}
