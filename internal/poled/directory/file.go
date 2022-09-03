package directory

import (
	"net/url"
	"pole/internal/poled/errors"
	"pole/internal/util/log"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
)

func FileIndexConfig(opts *IndexConfigArgs) bluge.Config {
	return bluge.DefaultConfigWithDirectory(func() index.Directory {
		return NewFileDirectoryWithUri(opts.Uri, opts.Logger)
	})
}

func NewFileDirectoryWithUri(uri string, logger *log.ZapLogger) index.Directory {
	logger = logger.WithField("uri", uri)
	u, err := url.Parse(uri)
	if err != nil {
		logger.Error(err.Error())
		return nil
	}
	if u.Scheme != SchemeType_name[SchemeTypeFile] {
		err := errors.ErrInvalidUri
		logger.Error(err.Error())
		return nil
	}
	return index.NewFileSystemDirectory(u.Path)
}
