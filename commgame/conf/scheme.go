package conf

import "github.com/Zentertain/zenlog"

type RedisConf struct {
	KeyPrefix    string `yaml:"KeyPrefix"`
	Addr         string `yaml:"Addr"`
	Pwd          string `yaml:"Pwd"`
	DB           int    `yaml:"DB"`
	PoolSize     int    `yaml:"PoolSize"`
	MinIdleConns int    `yaml:"MinIdleConns"`
}

type LogConfig struct {
	LevelStr   string `yaml:"levelStr"`
	ESUrl      string `yaml:"esUrl"`
	ESUserName string `yaml:"esUserName"`
	ESPassword string `yaml:"esPassword"`
	BiUrl      string `yaml:"biUrl"`
	MaxLogLen  uint64 `yaml:"maxLogLen"`
}

type LogstashConfig struct {
	Url        string `yaml:"url"`
	BufferSize int    `yaml:"bufferSize"`
}

const (
	MONGO_DEFAULT = "default"
)

const (
	ENV_DEV   = "dev"
	ENV_ALPHA = "alpha"
	ENV_BETA  = "beta"
	ENV_PROD  = "prod"
)

var Server struct {
	Host           string            `yaml:"Host"`
	RpcPort        int               `yaml:"RpcPort"`
	HttpPort       int               `yaml:"HttpPort"`   // For receive client http request.
	HttpCbPort     int               `yaml:"HttpCbPort"` // For apple / google server callback / notification.
	RedisPersist   RedisConf         `yaml:"RedisPersist"`
	RedisFileCache RedisConf         `yaml:"RedisFileCache"`
	MongoAddr      map[string]string `yaml:"MongoAddr"`
	ValidProject   []string          `yaml:"ValidProject"`
	SentryDSN      string            `yaml:"SentryDSN"`
	WeChatProxyURL string            `yaml:"WeChatProxyURL"`
	Env            string            `yaml:"Env"`
	Cloud          string            `yaml:"Cloud"`
	TracerUrl      string            `yaml:"TracerUrl"`
	ShutdownDelay  int               `yaml:"ShutdownDelay"`
	KeepCacheTime  int64             `yaml:"KeepCacheTime"` // 缓存保存时间,单位秒
	Log            LogConfig         `yaml:"log"`           // log的配置
	Logstash       LogstashConfig    `yaml:"logstash"`      // logstash 配置 发BI用
	LogCfg         *zenlog.Config    `yaml:"logCfg"`        // log新的配置
}

func initConfig() {
	Server.RpcPort = 9420
	Server.KeepCacheTime = 15 * 60
}

func IsTestEnv() bool {
	return Server.Env != ENV_PROD && Server.Env != ENV_BETA
}

func IsValidProject(projectId string) (isValid bool) {
	if len(Server.ValidProject) == 0 {
		return true
	}
	// 只load需要启动的project即可
	for _, validProject := range Server.ValidProject {
		if validProject == projectId {
			return true
		}
	}
	return false
}
