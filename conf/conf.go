package conf

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/Zentertain/zenlog"
	yaml "gopkg.in/yaml.v2"
)

const (
	EnvDev   = "dev"
	EnvAlpha = "alpha"
	EnvBeta  = "beta"
	EnvProd  = "prod"
)

type MongoConfig struct {
	DbName string `yaml:"DbName"` // 废弃不使用
	Addr   string `yaml:"Addr"`
}

type RedisConfig struct {
	Addr     string `yaml:"Addr"`
	Pwd      string `yaml:"Pwd"`
	DB       int    `yaml:"DB"`
	PoolSize int    `yaml:"PoolSize"`
}

type ServerConfig struct {
	Port string `yaml:"Port"`
}

type LogConfig struct {
	LevelStr   string `yaml:"levelStr"`
	ESUrl      string `yaml:"esUrl"`
	ESUserName string `yaml:"esUserName"`
	ESPassword string `yaml:"esPassword"`
	BIUrl      string `yaml:"biUrl"`
	MaxLogLen  uint64 `yaml:"maxLogLen"`
}

type CallBackProj struct {
	ID        string `yaml:"ID"`
	Proj      string `yaml:"Proj"`
	DatasetID string `yaml:"DatasetID"`
	TableID   string `yaml:"TableID"`
}

type PubSubProject struct {
	ID               string `yaml:"ID"`
	Project          string `yaml:"Project"`
	Subscription     string `yaml:"Subscription"`
	CallbackEndpoint string `yaml:"CallbackEndpoint"`
	SecretFileName   string `yaml:"SecretFileName"`
	Synchronous      bool   `yaml:"Synchronous"`
}

type AwsDynamoDBConfig struct {
	S3Bucket        string `yaml:"S3Bucket"`
	S3BucketFolder  string `yaml:"S3BucketFolder"`
	M3RecordsTable  string `yaml:"M3RecordsTable"`
	AccessKeyId     string `yaml:"AccessKeyId"`
	SecretAccessKey string `yaml:"SecretAccessKey"`
}

type PostgresConfig struct {
	Addr         string `yaml:"Addr"`
	MaxIdleConns int    `yaml:"MaxIdleConns"`
	MaxOpenConns int    `yaml:"MaxOpenConns"`
}

type PrivacyConf struct {
	MinGameVersion string `yaml:"MinGameVersion"`
	MaxGameVersion string `yaml:"MaxGameVersion"`
	Url            string `yaml:"Url"`
}

// function switch enum
const (
	Function_PubSubPullClient = "PubSubPullClient"
)

var ServerConf Config

type D1RetentionConfig map[string]*D1RetentionConfigItem

type D1RetentionConfigItem struct {
	AppToken   string `yaml:"AppToken"`
	EventToken string `yaml:"EventToken"`
	Env        string `yaml:"Env"`
}

type AuthConfig struct {
	SignKey    map[string]string `yaml:"SignKey"`
	AdminToken string            `yaml:"AdminToken"`
	Open       bool              `yaml:"Open"`
}

type SchedulerConfig struct {
	BaseInterval  int `yaml:"BaseInterval"`
	IntervalRange int `yaml:"IntervalRange"`
}

type Config struct {
	Mongo             *MongoConfig              `yaml:"Mongo"`
	Redis             *RedisConfig              `yaml:"Redis"`
	RedisDataFlow     *RedisConfig              `yaml:"RedisDataFlow"`
	RedisDataboatUser *RedisConfig              `yaml:"RedisDataboatUser"`
	Server            *ServerConfig             `yaml:"Server"`
	SentryDSN         string                    `yaml:"SentryDSN"`
	CallbackProjs     []*CallBackProj           `yaml:"CallbackProjs"`
	AwsDynamoDB       *AwsDynamoDBConfig        `yaml:"AwsDynamoDB"`
	PubSubProjs       []*PubSubProject          `yaml:"PubSubProjs"`
	Switch            map[string]bool           `yaml:"Switch"`
	Log               LogConfig                 `yaml:"log"` // log的配置
	ProjVersion       map[string]string         `yaml:"ProjVersion"`
	PgIap             *PostgresConfig           `yaml:"PgIap"`
	PgAccount         *PostgresConfig           `yaml:"PgAccount"`
	PgDataboat        *PostgresConfig           `yaml:"PgDataboat"`
	PgDataboatUser    *PostgresConfig           `yaml:"PgDataboatUser"`
	D1RetentionConfig *D1RetentionConfig        `yaml:"D1RetentionConfig"`
	PrivacyDefaultUrl string                    `yaml:"PrivacyDefaultUrl"`
	ProjPrivacy       map[string][]*PrivacyConf `yaml:"ProjPrivacy"`
	AuthCfg           AuthConfig                `yaml:"AuthCfg"`
	Env               string                    `yaml:"Env"`
	Cloud             string                    `yaml:"Cloud"`
	LogCfg            *zenlog.Config            `yaml:"logCfg"` // log新的配置
	Scheduler         *SchedulerConfig          `yaml:"Scheduler"`
}

func InitConf() {
	exec, err := os.Executable()
	if err != nil {
		panic("Get exec path fail!")
	}
	WorkingDir := filepath.Dir(exec)

	configFile := fmt.Sprintf("%s/conf/server.yaml", WorkingDir)
	err = readYaml(configFile, &ServerConf)
	if err != nil {
		panic(err)
	}

	fmt.Printf("server Conf: %+v\n", ServerConf)
}

func readYaml(filename string, out interface{}) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	return yaml.Unmarshal([]byte(data), out)
}

func FunctionOpen(funcName string) bool {
	if ServerConf.Switch == nil {
		return false
	}
	return ServerConf.Switch[funcName]
}

// GetSignKey 获取 jwt token的签名秘钥
func GetSignKey(project string) string {
	if ServerConf.AuthCfg.SignKey == nil {
		return ""
	}
	return ServerConf.AuthCfg.SignKey[project]
}

func IsDebugEnv() bool {
	return ServerConf.Env == EnvDev || ServerConf.Env == EnvAlpha || ServerConf.Env == EnvBeta
}

func IsProdEnv() bool {
	return ServerConf.Env == EnvProd
}

func IsDev() bool {
	return ServerConf.Env == EnvDev
}

func IsAlpha() bool {
	return ServerConf.Env == EnvAlpha
}

func IsBeta() bool {
	return ServerConf.Env == EnvBeta
}
