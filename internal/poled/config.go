package poled

import "sync"

const (
	defaultHttpAddr  = ":5000"
	defaultIndexPath = "/tmp/pole"
)

var confOnce sync.Once
var conf *Config

type Config struct {
	IndexPath string `mapstructure:"index_path"`
	HttpAddr  string `mapstructure:"http_addr"`
}

func GetConfig() *Config {
	confOnce.Do(func() {
		conf = &Config{}
	})
	return conf
}

func GetHttpAddr() string {
	rs := conf.HttpAddr
	if rs == "" {
		rs = defaultHttpAddr
	}
	return rs
}

func GetIndexPath() string {
	rs := conf.IndexPath
	if rs == "" {
		rs = defaultIndexPath
	}
	return rs
}
