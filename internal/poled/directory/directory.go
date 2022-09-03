package directory

import (
	"fmt"
	"net/url"
	"pole/internal/poled/errors"
	"pole/internal/util/log"

	"github.com/blugelabs/bluge"
)

type uint64Slice []uint64

func (e uint64Slice) Len() int           { return len(e) }
func (e uint64Slice) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e uint64Slice) Less(i, j int) bool { return e[i] < e[j] }

type SchemeType int

const (
	SchemeTypeUnknown SchemeType = iota
	SchemeTypeFile
	SchemeTypeOss
)

var (
	SchemeType_name = map[SchemeType]string{
		SchemeTypeUnknown: "unknown",
		SchemeTypeFile:    "file",
		SchemeTypeOss:     "oss",
	}
	SchemeType_value = map[string]SchemeType{
		"file": SchemeTypeFile,
		"oss":  SchemeTypeOss,
	}
)

type Lock interface {
	Lock() error
	Unlock() error
}

type IndexConfigArgs struct {
	Uri    string
	Lock   Lock
	Logger *log.ZapLogger
}

type option func(op *IndexConfigArgs)

func WithLock(lock Lock) option {
	return func(op *IndexConfigArgs) {
		op.Lock = lock
	}
}

func NewIndexConfigWithUri(uri string, options ...option) (bluge.Config, error) {
	var rs bluge.Config
	url, err := url.Parse(uri)
	if err != nil {
		return rs, fmt.Errorf("%w:%s", errors.ErrInvalidUri, err.Error())
	}

	args := &IndexConfigArgs{
		Uri:    uri,
		Logger: log.WithField("module", "directory"),
	}

	for _, op := range options {
		op(args)
	}

	switch SchemeType_value[url.Scheme] {
	case SchemeTypeFile:
		return FileIndexConfig(args), nil
	case SchemeTypeOss:
		return OssIndexConfig(args), nil
	}
	return rs, errors.ErrUnSupportedSchemeType
}
