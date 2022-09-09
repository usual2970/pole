package directory

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"path"
	"path/filepath"
	"pole/internal/poled/errors"
	"pole/internal/util/log"
	"strconv"
	"strings"
	"time"

	"github.com/rs/xid"

	"pole/internal/poled/clients"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	segment "github.com/blugelabs/bluge_segment_api"
	"go.uber.org/zap"
)

func OssIndexConfig(args *IndexConfigArgs) bluge.Config {
	return bluge.DefaultConfigWithDirectory(func() index.Directory {
		return NewOssDirectoryWithUri(args)
	})
}

type OssDirectory struct {
	bucket         string
	bucketCli      *oss.Bucket
	path           string
	client         *oss.Client
	ctx            context.Context
	requestTimeout time.Duration
	lock           Lock
	lockUri        string
	logger         *log.ZapLogger
}

func NewOssDirectoryWithUri(opts *IndexConfigArgs) *OssDirectory {
	directoryLogger := opts.Logger.WithField("schemeType", "oss")

	client, err := clients.NewOssClientWithUri(opts.Uri)
	if err != nil {
		opts.Logger.Error(err.Error(), zap.String("uri", opts.Uri))
		return nil
	}

	// Parse URI.
	u, err := url.Parse(opts.Uri)
	if err != nil {
		opts.Logger.Error(err.Error(), zap.String("uri", opts.Uri))
		return nil
	}
	if u.Scheme != SchemeType_name[SchemeTypeOss] {
		err := errors.ErrInvalidUri
		opts.Logger.Error(err.Error(), zap.String("uri", opts.Uri))
		return nil
	}

	return &OssDirectory{
		client:         client,
		bucket:         u.Host,
		path:           path.Join(u.Path, opts.Idx),
		ctx:            context.Background(),
		requestTimeout: 3 * time.Second,
		lock:           opts.Lock,
		lockUri:        xid.New().String(),
		logger:         directoryLogger,
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

	objects, err := d.bucketCli.ListObjectsV2(oss.Prefix(strings.TrimLeft(d.path, "/")))
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

func (d *OssDirectory) fullFilePath(kind string, id uint64) string {
	path := filepath.Join(d.path, d.fileName(kind, id))

	path = strings.TrimLeft(strings.Replace(path, "\\", "/", -1), "/")
	return path
}

func (d *OssDirectory) Load(kind string, id uint64) (*segment.Data, io.Closer, error) {
	path := d.fullFilePath(kind, id)
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

	path := d.fullFilePath(kind, id)
	err = d.bucketCli.PutObject(path, reader, oss.ContentType("application/octet-stream"))
	if err != nil {
		d.logger.Error(err.Error(), zap.String("bucket", d.bucket), zap.String("path", path), zap.Int64("size", size))

		// TODO: Remove the failed file.
		return err
	}

	return nil
}

func (d *OssDirectory) Remove(kind string, id uint64) error {
	path := d.fullFilePath(kind, id)
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
	return d.lock.Lock(d.lockUri)
}
func (d *OssDirectory) Unlock() error {
	return d.lock.Unlock(d.lockUri)
}
