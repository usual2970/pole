package poled

import "errors"

type result interface {
	Data() interface{}
	Error() error
}

var (
	ErrIndexExist         = errors.New("index already exists")
	ErrIndexNotFound      = errors.New("index not found")
	ErrWriterNotFound     = errors.New("writer not found")
	ErrSyntaxNotSupported = errors.New("syntax not supported")
	ErrBatchFailed        = errors.New("batch failed")
)

type generalResult struct {
	err error
}

func newGeneralResult(err error) *generalResult {
	return &generalResult{
		err: err,
	}
}

func (r *generalResult) Data() interface{} {
	return nil
}

func (r *generalResult) Error() error {
	return r.err
}

type createResult struct {
	err error
}

func (r *createResult) Data() interface{} {
	return nil
}

func (r *createResult) Error() error {
	return r.err
}
