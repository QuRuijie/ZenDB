package zdb

import (
	"github.com/QuRuijie/zenDB/commgame/conf"
	"github.com/Zentertain/zenlog"
	"time"

	"github.com/go-redis/redis"
)

type ZRedisClient struct {
	*redis.Client
	clientName string
}

var Persist ZRedisClient
var RedisFileCache ZRedisClient

func InitRedis() {
	Persist = ZRedisClient{newRedisClient(conf.Server.RedisPersist), "persist"}
	RedisFileCache = ZRedisClient{newRedisClient(conf.Server.RedisFileCache), "RedisFileCache"}
}

func newRedisClient(redisConf conf.RedisConf) *redis.Client {
	r := redis.NewClient(&redis.Options{
		Addr:         redisConf.Addr,
		Password:     redisConf.Pwd,
		DB:           redisConf.DB,
		PoolSize:     redisConf.PoolSize,
		MinIdleConns: redisConf.MinIdleConns,
	})
	_, err := r.Ping().Result()
	if err != nil {
		zenlog.Error("redis cannot connect:%+v, %+v", err, redisConf)
	} else {
		zenlog.Info("redis connect ok:%+v", redisConf.Addr)
	}
	return r
}

func (client ZRedisClient) ZRankX(key, member string, rev bool) *redis.IntCmd {
	if rev {
		return client.ZRevRank(key, member)
	} else {
		return client.ZRank(key, member)
	}
}

func (client ZRedisClient) ZRangeWithScoresX(key string, start, stop int64, rev bool) *redis.ZSliceCmd {
	if rev {
		return client.ZRevRangeWithScores(key, start, stop)
	} else {
		return client.ZRangeWithScores(key, start, stop)
	}
}

func (client ZRedisClient) ZRangeX(key string, start, stop int64, rev bool) *redis.StringSliceCmd {
	if rev {
		return client.ZRevRange(key, start, stop)
	} else {
		return client.ZRange(key, start, stop)
	}
}

// redis support unlink from 4.0
func (client ZRedisClient) Unlink(key ...string) *redis.IntCmd {
	cmd := client.Client.Unlink(key...)
	if cmd.Err() != nil {
		return client.Del(key...)
	}

	return cmd
}

func (client ZRedisClient) Del(keys ...string) *redis.IntCmd {
	return client.Client.Del(keys...)
}

func (client ZRedisClient) HSet(key, field string, value interface{}) *redis.BoolCmd {
	return client.Client.HSet(key, field, value)
}

func (client ZRedisClient) HDel(key string, fields ...string) *redis.IntCmd {
	return client.Client.HDel(key, fields...)
}

func (client ZRedisClient) HGet(key, field string) *redis.StringCmd {
	return client.Client.HGet(key, field)
}

func (client ZRedisClient) HGetAll(key string) *redis.StringStringMapCmd {
	return client.Client.HGetAll(key)
}

func (client ZRedisClient) HScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	return client.Client.HScan(key, cursor, match, count)
}

func (client ZRedisClient) HSetNX(key, field string, value interface{}) *redis.BoolCmd {
	return client.Client.HSetNX(key, field, value)
}

func (client ZRedisClient) HMSet(key string, fields map[string]interface{}) *redis.StatusCmd {
	return client.Client.HMSet(key, fields)
}

func (client ZRedisClient) HIncrBy(key, field string, incr int64) *redis.IntCmd {
	return client.Client.HIncrBy(key, field, incr)
}

func (client ZRedisClient) Keys(pattern string) *redis.StringSliceCmd {
	return client.Client.Keys(pattern)
}

func (client ZRedisClient) Publish(channel string, message interface{}) *redis.IntCmd {
	return client.Client.Publish(channel, message)
}

func (client ZRedisClient) Subscribe(channels ...string) *redis.PubSub {
	return client.Client.Subscribe(channels...)
}

func (client ZRedisClient) SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return client.Client.SetNX(key, value, expiration)
}

func (client ZRedisClient) ZRem(key string, members ...interface{}) *redis.IntCmd {
	return client.Client.ZRem(key, members...)
}

func (client ZRedisClient) ZAdd(key string, members ...redis.Z) *redis.IntCmd {
	return client.Client.ZAdd(key, members...)
}

func (client ZRedisClient) ZScore(key, member string) *redis.FloatCmd {
	return client.Client.ZScore(key, member)
}

func (client ZRedisClient) ZCount(key, min, max string) *redis.IntCmd {
	return client.Client.ZCount(key, min, max)
}

func (client ZRedisClient) ZRemRangeByRank(key string, start, stop int64) *redis.IntCmd {
	return client.Client.ZRemRangeByRank(key, start, stop)
}

func (client ZRedisClient) ZCard(key string) *redis.IntCmd {
	return client.Client.ZCard(key)
}

func (client ZRedisClient) Incr(key string) *redis.IntCmd {
	return client.Client.Incr(key)
}

func (client ZRedisClient) SCard(key string) *redis.IntCmd {
	return client.Client.SCard(key)
}

func (client ZRedisClient) SAdd(key string, members ...interface{}) *redis.IntCmd {
	return client.Client.SAdd(key, members...)
}

func (client ZRedisClient) Scan(cursor uint64, match string, count int64) *redis.ScanCmd {
	return client.Client.Scan(cursor, match, count)
}

func (client ZRedisClient) GetPoolConnections() uint32 {
	poolStats := client.Client.PoolStats()
	conns := poolStats.TotalConns - poolStats.IdleConns
	if int32(conns) < 0 {
		conns = 0
	}
	return conns
}

func (client ZRedisClient) Get(key string) *redis.StringCmd {
	return client.Client.Get(key)
}

func (client ZRedisClient) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return client.Client.Set(key, value, expiration)
}

func (client ZRedisClient) HMGet(key string, fields ...string) *redis.SliceCmd {
	return client.Client.HMGet(key, fields...)
}

func (client ZRedisClient) ZAddXX(key string, members ...redis.Z) *redis.IntCmd {
	return client.Client.ZAddXX(key, members...)
}

func (client ZRedisClient) ZAddNX(key string, members ...redis.Z) *redis.IntCmd {
	return client.Client.ZAddNX(key, members...)
}

// redis support zadd for option from 3.0.2
func (client ZRedisClient) ZIncr(key string, member redis.Z) *redis.FloatCmd {
	cmd := client.Client.ZIncr(key, member)
	if cmd.Err() != nil {
		return client.ZIncrBy(key, member.Score, member.Member.(string))
	}
	return cmd
}

func (client ZRedisClient) ZIncrBy(key string, increment float64, member string) *redis.FloatCmd {
	return client.Client.ZIncrBy(key, increment, member)
}

func (client ZRedisClient) ExpireAt(key string, tm time.Time) *redis.BoolCmd {
	return client.Client.ExpireAt(key, tm)
}

func (client ZRedisClient) HExists(key, field string) *redis.BoolCmd {
	return client.Client.HExists(key, field)
}

func (client ZRedisClient) Rename(key, newkey string) *redis.StatusCmd {
	return client.Client.Rename(key, newkey)
}

func (client ZRedisClient) Expire(key string, expiration time.Duration) *redis.BoolCmd {
	return client.Client.Expire(key, expiration)
}

func (client ZRedisClient) SIsMember(key string, member interface{}) *redis.BoolCmd {
	return client.Client.SIsMember(key, member)
}

func (client ZRedisClient) SMembers(key string) *redis.StringSliceCmd {
	return client.Client.SMembers(key)
}

func (client ZRedisClient) SRem(key string, members ...interface{}) *redis.IntCmd {
	return client.Client.SRem(key, members...)
}
