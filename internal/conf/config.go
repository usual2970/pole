package conf

import "sync"

const (
	defaultHttpAddr  = ":5000"
	defaultGrpcAddr  = ":5001"
	defaultIndexPath = "file:///tmp/pole"
	defaultDataPath  = "./"
)

var confOnce sync.Once
var conf *Config

type RaftConfig struct {
	Id        string `mapstructure:"id"`
	BootStrap bool   `mapstructure:"bootstrap"`
	Address   string `mapstructure:"address"`
	DataDir   string `mapstructure:"data_dir"`
}

type Config struct {
	IndexUri string     `mapstructure:"index_uri"`
	HttpAddr string     `mapstructure:"http_addr"`
	GrpcAddr string     `mapstructure:"grpc_addr"`
	DataPath string     `mapstructure:"data_path"`
	Raft     RaftConfig `mapstructure:"raft"`
	Join     string     `mapstructure:"join"`
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

func GetGrpcAddr() string {
	rs := conf.GrpcAddr
	if rs == "" {
		rs = defaultGrpcAddr
	}
	return rs
}

func GetIndexPath() string {
	rs := conf.IndexUri
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

func SetJoin(join string) {
	conf.Join = join
}
