package directory

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"pole/internal/poled/errors"
	"pole/internal/util/log"
	"strconv"
	"strings"
	"time"

	"pole/internal/poled/clients"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	segment "github.com/blugelabs/bluge_segment_api"
	"go.uber.org/zap"
)

func OssIndexConfig(uri string, lockUri string, logger *log.ZapLogger) bluge.Config {
	return bluge.DefaultConfigWithDirectory(func() index.Directory {
		return NewOssDirectoryWithUri(uri, lockUri, logger)
	})
}

type OssDirectory struct {
	bucket         string
	bucketCli      *oss.Bucket
	path           string
	client         *oss.Client
	ctx            context.Context
	requestTimeout time.Duration
	lockUri        string
	logger         *log.ZapLogger
}

func NewOssDirectoryWithUri(uri string, lockUri string, logger *log.ZapLogger) *OssDirectory {
	directoryLogger := logger.WithField("schemeType", "oss")

	client, err := clients.NewOssClientWithUri(uri)
	if err != nil {
		logger.Error(err.Error(), zap.String("uri", uri))
		return nil
	}

	// Parse URI.
	u, err := url.Parse(uri)
	if err != nil {
		logger.Error(err.Error(), zap.String("uri", uri))
		return nil
	}
	if u.Scheme != SchemeType_name[SchemeTypeOss] {
		err := errors.ErrInvalidUri
		logger.Error(err.Error(), zap.String("uri", uri))
		return nil
	}

	return &OssDirectory{
		client:         client,
		bucket:         u.Host,
		path:           u.Path,
		ctx:            context.Background(),
		requestTimeout: 3 * time.Second,
		lockUri:        lockUri,

		logger: directoryLogger,
	}
}

func (d *OssDirectory) exists() (bool, error) {
	rs, err := d.client.IsBucketExist(d.bucket)
	if err != nil {
		d.logger.Error(err.Error(), zap.String("bucket", d.bucket))
		return false, err
	}
	return rs, nil
}

func (d *OssDirectory) Setup(readOnly bool) error {
	exists, err := d.exists()
	if err != nil {
		d.logger.Error(err.Error(), zap.String("bucket", d.bucket))
		return err
	}

	if !exists {
		if err = d.client.CreateBucket(d.bucket, oss.ACL(oss.ACLPublicReadWrite)); err != nil {
			d.logger.Error(err.Error(), zap.String("bucket", d.bucket))
			return err
		}
	}

	bucketCli, err := d.client.Bucket(d.bucket)
	if err != nil {
		d.logger.Error(err.Error(), zap.String("bucket", d.bucket))
		return err
	}

	d.bucketCli = bucketCli
	return nil
}

func (d *OssDirectory) List(kind string) ([]uint64, error) {

	objects, err := d.bucketCli.ListObjectsV2()
	if err != nil {
		return nil, err
	}
	var rv uint64Slice
	for _, object := range objects.Objects {
		if filepath.Ext(object.Key) != kind {
			continue
		}

		base := filepath.Base(object.Key)
		base = base[:len(base)-len(kind)]

		var epoch uint64
		epoch, err := strconv.ParseUint(base, 16, 64)
		if err != nil {
			d.logger.Error(err.Error(), zap.String("base", base))
			return nil, err
		}
		rv = append(rv, epoch)
	}
	return rv, nil
}

func (d *OssDirectory) fileName(kind string, id uint64) string {
	return fmt.Sprintf("%012x", id) + kind
}

func (d *OssDirectory) Load(kind string, id uint64) (*segment.Data, io.Closer, error) {
	path := filepath.Join(d.path, d.fileName(kind, id))
	path = strings.TrimLeft(path, "/")
	object, err := d.bucketCli.GetObject(path)
	if err != nil {
		d.logger.Error(err.Error(), zap.String("bucket", d.bucket), zap.String("path", path))
		return nil, nil, err
	}

	data, err := ioutil.ReadAll(object)
	if err != nil {
		d.logger.Error(err.Error())
		return nil, nil, err
	}

	return segment.NewDataBytes(data), nil, nil
}

func (d *OssDirectory) Persist(kind string, id uint64, w index.WriterTo, closeCh chan struct{}) error {
	var buf bytes.Buffer
	size, err := w.WriteTo(&buf, closeCh)
	if err != nil {
		d.logger.Error(err.Error())
		return err
	}

	reader := bufio.NewReader(&buf)

	path := filepath.Join(d.path, d.fileName(kind, id))
	path = strings.TrimLeft(path, "/")
	err = d.bucketCli.PutObject(path, reader, oss.ContentType("application/octet-stream"))
	if err != nil {
		d.logger.Error(err.Error(), zap.String("bucket", d.bucket), zap.String("path", path), zap.Int64("size", size))

		// TODO: Remove the failed file.
		return err
	}

	return nil
}

func (d *OssDirectory) Remove(kind string, id uint64) error {
	path := filepath.Join(d.path, d.fileName(kind, id))
	path = strings.TrimLeft(path, "/")
	if err := d.bucketCli.DeleteObject(path); err != nil {
		d.logger.Error(err.Error(), zap.String("bucket", d.bucket), zap.String("path", path))
		return err
	}
	return nil
}

func (d *OssDirectory) Stats() (numItems uint64, numBytes uint64) {

	numFilesOnDisk := uint64(0)
	numBytesUsedDisk := uint64(0)

	objects, err := d.bucketCli.ListObjectsV2()
	if err != nil {
		d.logger.Error(err.Error(), zap.String("bucket", d.bucket))
	}

	for _, object := range objects.Objects {
		numFilesOnDisk++
		numBytesUsedDisk += uint64(object.Size)
	}

	return numFilesOnDisk, numBytesUsedDisk
}
func (d *OssDirectory) Sync() error {
	return nil
}

func (d *OssDirectory) Lock() error {
	// Create lock manager
	return nil
}
func (d *OssDirectory) Unlock() error {
	return nil
}
