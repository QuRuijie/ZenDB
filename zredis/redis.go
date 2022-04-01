package zredis

import (
	"github.com/QuRuijie/zenDB/prom"
	"github.com/Zentertain/zenlog"
	"time"

	"github.com/go-redis/redis"
)

const (
	SPIN_TIME = 10
	SPIN_NUM  = 10
)

type RedisClient struct {
	*redis.Client
	clientName string
}

type RedisPipeliner struct {
	redis.Pipeliner
}

//------------------------------------Public----------------------------------------

func NewRedisClient(opt *redis.Options) *redis.Client {
	r := redis.NewClient(opt)
	_, err := r.Ping().Result()
	if err != nil {
		zenlog.Fatal("zredis cannot connect:%+v", err)
	}
	return r
}

func NewClient(opt *redis.Options, clientName string) *RedisClient {
	return &RedisClient{NewRedisClient(opt), clientName}
}

func (c RedisClient) Keys(pattern string) (cmd *redis.StringSliceCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.Keys(pattern)
}

func (c RedisClient) Scan(cursor uint64, match string, count int64) (cmd *redis.ScanCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.Scan(cursor, match, count)
}

func (c RedisClient) Publish(channel string, message interface{}) (cmd *redis.IntCmd) {
	return c.Client.Publish(channel, message)
}

func (c RedisClient) Subscribe(channels ...string) (cmd *redis.PubSub) {
	return c.Client.Subscribe(channels...)
}

func (c RedisClient) GetPoolConnections() uint32 {
	poolStats := c.Client.PoolStats()
	cons := poolStats.TotalConns - poolStats.IdleConns
	if int32(cons) < 0 {
		cons = 0
	}
	return cons
}

func (c RedisClient) Pipeline() RedisPipeliner {
	return RedisPipeliner{c.Client.Pipeline()}
}

func (pipe RedisPipeliner) Exec() (cmd []redis.Cmder, err error) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, err) }(prom.NowMillisecond())
	return pipe.Pipeliner.Exec()
}

func SetJobMux(r *RedisClient, key, val string, expireTime time.Duration) bool {
	return SetJobMuxWithSpins(r, key, val, expireTime, SPIN_TIME, SPIN_NUM)
}

func SetJobMuxWithSpins(r *RedisClient, key, val string, expireTime, spinTime time.Duration, spinNum int) bool {
	for i := 0; i < spinNum; i++ {
		isSet := r.SetNX(key, val, expireTime)
		if isSet.Val() {
			return true
		}
		// spin 10ms
		time.Sleep(spinTime)
	}
	return false
}

func ReleaseJobMux(r *RedisClient, key string) {
	r.Unlink(key)
}

//------------------------------------Key----------------------------------------

func (c RedisClient) Get(key string) (cmd *redis.StringCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.Get(key)
}

func (c RedisClient) Set(key string, value interface{}, expiration time.Duration) (cmd *redis.StatusCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.Set(key, value, expiration)
}

func (c RedisClient) Del(keys ...string) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.Del(keys...)
}

// ?
func (c RedisClient) Unlink(key ...string) (cmd *redis.IntCmd) {
	//redis support unlink from 4.0
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	cmd = c.Client.Unlink(key...)
	if cmd.Err() != nil {
		return c.Del(key...)
	}
	return
}

func (c RedisClient) Incr(key string) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.Incr(key)
}

func (c RedisClient) SetNX(key string, value interface{}, expiration time.Duration) (cmd *redis.BoolCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.SetNX(key, value, expiration)
}

func (c RedisClient) Expire(key string, expiration time.Duration) (cmd *redis.BoolCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.Expire(key, expiration)
}

func (c RedisClient) ExpireAt(key string, tm time.Time) (cmd *redis.BoolCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.ExpireAt(key, tm)
}

func (c RedisClient) Rename(key, newkey string) (cmd *redis.StatusCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.Rename(key, newkey)
}

//------------------------------------Hash------------------------------------------

func (c RedisClient) HSet(key, field string, value interface{}) (cmd *redis.BoolCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.HSet(key, field, value)
}

func (c RedisClient) HGet(key, field string) (cmd *redis.StringCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.HGet(key, field)
}

func (c RedisClient) HDel(key string, fields ...string) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.HDel(key, fields...)
}

func (c RedisClient) HExists(key, field string) (cmd *redis.BoolCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.HExists(key, field)
}

func (c RedisClient) HGetAll(key string) (cmd *redis.StringStringMapCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.HGetAll(key)
}

func (c RedisClient) HIncrBy(key, field string, incr int64) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.HIncrBy(key, field, incr)
}

func (c RedisClient) HMSet(key string, fields map[string]interface{}) (cmd *redis.StatusCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.HMSet(key, fields)
}

func (c RedisClient) HMGet(key string, fields ...string) (cmd *redis.SliceCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.HMGet(key, fields...)
}

func (c RedisClient) HSetNX(key, field string, value interface{}) (cmd *redis.BoolCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.HSetNX(key, field, value)
}

func (c RedisClient) HScan(key string, cursor uint64, match string, count int64) (cmd *redis.ScanCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.HScan(key, cursor, match, count)
}

//------------------------------------Set-------------------------------------------

func (c RedisClient) SAdd(key string, members ...interface{}) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.SAdd(key, members...)
}

func (c RedisClient) SCard(key string) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.SCard(key)
}

func (c RedisClient) SIsMember(key string, member interface{}) (cmd *redis.BoolCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.SIsMember(key, member)
}

func (c RedisClient) SMembers(key string) (cmd *redis.StringSliceCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.SMembers(key)
}

func (c RedisClient) SRem(key string, members ...interface{}) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.SRem(key, members...)
}

//------------------------------------ZSet------------------------------------------

func (c RedisClient) ZAdd(key string, members ...redis.Z) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.ZAdd(key, members...)
}

func (c RedisClient) ZScore(key, member string) (cmd *redis.FloatCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.ZScore(key, member)
}

func (c RedisClient) ZAddXX(key string, members ...redis.Z) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.ZAddXX(key, members...)
}

func (c RedisClient) ZAddNX(key string, members ...redis.Z) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.ZAddNX(key, members...)
}

func (c RedisClient) ZCard(key string) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.ZCard(key)
}

func (c RedisClient) ZRem(key string, members ...interface{}) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.ZRem(key, members...)
}

func (c RedisClient) ZCount(key, min, max string) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.ZCount(key, min, max)
}

// ?
//ZIncr redis support zadd for option from 3.0.2 ,equal ZAdd if member.Member not exist
func (c RedisClient) ZIncr(key string, member redis.Z) (cmd *redis.FloatCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	cmd = c.Client.ZIncr(key, member)
	if cmd.Err() != nil {
		return c.ZIncrBy(key, member.Score, member.Member.(string))
	}
	return cmd
}

func (c RedisClient) ZIncrBy(key string, increment float64, member string) (cmd *redis.FloatCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.ZIncrBy(key, increment, member)
}

//ZRankX 返回正序 or 倒序
func (c RedisClient) ZRankX(key, member string, rev bool) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	if rev {
		return c.Client.ZRevRank(key, member) //返回member排名
	}
	return c.Client.ZRank(key, member) //返回索引
}

//ZRangeWithScoresX 返回正序 or 倒序的名次范围的zSet
func (c RedisClient) ZRangeWithScoresX(key string, start, stop int64, rev bool) (cmd *redis.ZSliceCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	if rev {
		return c.Client.ZRevRangeWithScores(key, start, stop)
	}
	return c.Client.ZRangeWithScores(key, start, stop)
}

//ZRangeX 返回正序 or 倒序的名次范围的zSet.Member
func (c RedisClient) ZRangeX(key string, start, stop int64, rev bool) (cmd *redis.StringSliceCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	if rev {
		return c.Client.ZRevRange(key, start, stop)
	}
	return c.Client.ZRange(key, start, stop)
}

//ZRemRangeByRank 根据倒序排名移出
func (c RedisClient) ZRemRangeByRank(key string, start, stop int64) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.ZRemRangeByRank(key, start, stop)
}

//------------------------------------List------------------------------------------

func (c RedisClient) LPush(key string, values ...interface{}) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.LPush(key, values...)
}

func (c RedisClient) RPush(key string, values ...interface{}) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.RPush(key, values...)
}

func (c RedisClient) LLen(key string) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.LLen(key)
}

func (c RedisClient) LPop(key string) (cmd *redis.StringCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.LPop(key)
}

func (c RedisClient) RPop(key string) (cmd *redis.StringCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.RPop(key)
}

func (c RedisClient) LRem(key string, count int64, value interface{}) (cmd *redis.IntCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.LRem(key, count, value)
}

func (c RedisClient) LIndex(key string, index int64) (cmd *redis.StringCmd) {
	defer func(startTime int64) { prom.SetRedisMetrics(startTime, cmd.Err()) }(prom.NowMillisecond())
	return c.Client.LIndex(key, index)
}

//------------------------------------End-------------------------------------------

//// 定时获取redis的连接池当前连接数，并计算最大连接数
//func InitPoolConnectionsMetrics() {
//	if conf.Server.MetricsConfig == nil || !conf.Server.MetricsConfig.Enable || conf.Server.MetricsConfig.Interval == 0 {
//		return
//	}
//	ticker := time.NewTicker(time.Second * 1)
//	go func() {
//		for {
//			//monitor.RedisMetric.ResetMaxPoolConnections()
//			startTime := time.Now()
//			for ; true; <-ticker.C {
//				if time.Since(startTime).Seconds() >= float64(monitor.TotalRedisMetric.Sec) {
//					break
//				}
//				for _, client := range redisClients {
//					connections := client.GetPoolConnections()
//					if connections > monitor.TotalRedisMetric.RedisMetric(client.clientName).MaxPoolConnections() {
//						monitor.TotalRedisMetric.RedisMetric(client.clientName).SetMaxPoolConnections(connections)
//					}
//				}
//			}
//		}
//	}()
//}
