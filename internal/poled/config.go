package poled

import "sync"

const (
	defaultHttpAddr  = ":5000"
	defaultIndexPath = "/tmp/pole"
	defaultDataPath  = "./"
)

var confOnce sync.Once
var conf *Config

type Config struct {
	IndexPath string `mapstructure:"index_path"`
	HttpAddr  string `mapstructure:"http_addr"`
	DataPath  string `mapstructure:"data_path"`
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

func GetDataPath() string {
	rs := conf.DataPath
	if rs == "" {
		rs = defaultDataPath
	}
	return rs
}
