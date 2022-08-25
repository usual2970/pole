package poled

import (
	"errors"
	"net/http"
	mt "pole/internal/poled/meta"
	"pole/internal/poled/sql"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
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

func newSelectResult(iter search.DocumentMatchIterator, meta mt.Mapping, cols []sql.Col, selectAll bool) *selectResp {

	hitItems := make([]Hit, 0, iter.Aggregations().Count())
	colsMap := cols2Map(cols)
	next, err := iter.Next()
	for err == nil && next != nil {
		hit := Hit{
			Source: make(map[string]interface{}),
		}
		_ = next.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_id" {
				hit.ID = string(value)
				return true
			}

			v, ok := parseValue(field, value, meta, colsMap, selectAll)
			if ok {
				hit.Source[field] = v
			}

			return true
		})
		hitItems = append(hitItems, hit)

		next, err = iter.Next()
	}

	hits := Hits{
		Total:    int64(iter.Aggregations().Count()),
		MaxScore: iter.Aggregations().Metric("max_score"),
		Hits:     hitItems,
	}

	rs := &selectResp{
		Took:     iter.Aggregations().Duration().Milliseconds(),
		TimedOut: false,
		Hits:     hits,
	}

	return rs
}

type Hits struct {
	Total    int64   `json:"total"`
	MaxScore float64 `json:"max_score"`
	Hits     []Hit   `json:"hits"`
}

type Hit struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
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

func cols2Map(cols []sql.Col) map[string]sql.Col {
	rs := make(map[string]sql.Col)
	for _, col := range cols {
		rs[col.Name] = col
	}
	return rs
}

func parseValue(field string, value []byte, meta mt.Mapping, cols map[string]sql.Col, selectAll bool) (interface{}, bool) {
	if _, ok := cols[field]; !ok && !selectAll {
		return nil, false
	}

	fieldOption, ok := meta.Properties[field]
	if !ok {
		return nil, false
	}

	switch fieldOption.Type {
	case mt.FieldTypeNumeric:
		v, _ := bluge.DecodeNumericFloat64(value)
		return v, true
	case mt.FieldTypeText:
		return string(value), true
	}
	return nil, false
}
