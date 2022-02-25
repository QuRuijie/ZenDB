package db

import (
	"github.com/QuRuijie/zenDB/conf"
	"github.com/Zentertain/zenlog"
)

var (
	Mongo             *MongoClient
	Redis             *RedisClient
	RedisDataFlow     *RedisClient
	RedisDataboatUser *RedisClient
)

// InitMongo 初始化mongo数据库
func InitMongo() {
	Mongo = NewMongoClient(conf.ServerConf.Mongo)
	if Mongo != nil {
		// 调用该接口会尝试创建索引
		Mongo.Database(AdjustDB)
		Mongo.Database(DataBoatDB)
	}
}

func InitRedis(redis *conf.RedisConfig, redisDataFlow *conf.RedisConfig) {
	if redis == nil {
		zenlog.Info("no redis")
		return
	}
	zenlog.Debug("redis conf:%+v", redis)
	zenlog.Debug("data flow redis conf:%+v", redisDataFlow)

	Redis = NewRedisClient(&RedisOption{
		Addr:     redis.Addr,
		Pwd:      redis.Pwd,
		DB:       redis.DB,
		PoolSize: redis.PoolSize,
	})

	if redisDataFlow == nil {
		return
	}

	// init redis client for dataflow transfer to tencent redis

	Redis = NewRedisClient(&RedisOption{
		Addr:     redisDataFlow.Addr,
		Pwd:      redisDataFlow.Pwd,
		DB:       redisDataFlow.DB,
		PoolSize: redisDataFlow.PoolSize,
	})
}

func InitRedisDataboatUser(redis *conf.RedisConfig) {
	if redis == nil {
		zenlog.Info("no redis")
		return
	}

	RedisDataboatUser = NewRedisClient(&RedisOption{
		Addr:     redis.Addr,
		Pwd:      redis.Pwd,
		DB:       redis.DB,
		PoolSize: redis.PoolSize,
	})
}
