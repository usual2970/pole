package clients

import (
	"net/url"
	"os"
	"pole/internal/poled/errors"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

func NewOssClientWithUri(uri string) (*oss.Client, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "oss" {
		return nil, errors.ErrInvalidUri
	}

	endpoint := os.Getenv("OSS_ENDPOINT_URL")
	if str := u.Query().Get("endpoint"); str != "" {
		endpoint = str
	}

	accessKeyId := os.Getenv("OSS_ACCESS_KEY_ID")
	if str := u.Query().Get("access_key_id"); str != "" {
		accessKeyId = str
	}

	accessKeySecret := os.Getenv("OSS_ACCESS_KEY_SECRET")
	if str := u.Query().Get("access_key_secret"); str != "" {
		accessKeySecret = str
	}

	return oss.New(endpoint, accessKeyId, accessKeySecret)
}
