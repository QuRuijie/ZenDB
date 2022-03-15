package zredis

import (
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

func newRedisClient(opt *redis.Options) *redis.Client {
	r := redis.NewClient(opt)
	_, err := r.Ping().Result()
	if err != nil {
		zenlog.Fatal("zredis cannot connect:%+v", err)
	}
	return r
}

func newClient(opt *redis.Options, clientName string) *RedisClient {
	return &RedisClient{newRedisClient(opt), clientName}
}

func (c RedisClient) Keys(pattern string) *redis.StringSliceCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_keys")
	return c.Client.Keys(pattern)
}

func (c RedisClient) Publish(channel string, message interface{}) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_publish")
	return c.Client.Publish(channel, message)
}

func (c RedisClient) Subscribe(channels ...string) *redis.PubSub {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_subscribe")
	return c.Client.Subscribe(channels...)
}

func (c RedisClient) Scan(cursor uint64, match string, count int64) *redis.ScanCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_scan")
	return c.Client.Scan(cursor, match, count)
}

func (c RedisClient) GetPoolConnections() uint32 {
	poolStats := c.Client.PoolStats()
	conns := poolStats.TotalConns - poolStats.IdleConns
	if int32(conns) < 0 {
		conns = 0
	}
	return conns
}

func (c RedisClient) Pipeline() RedisPipeliner {
	return RedisPipeliner{c.Client.Pipeline()}
}

func (pipe RedisPipeliner) Exec() ([]redis.Cmder, error) {
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

func (c RedisClient) Get(key string) *redis.StringCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_get")
	return c.Client.Get(key)
}

func (c RedisClient) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_set")
	return c.Client.Set(key, value, expiration)
}

func (c RedisClient) Del(keys ...string) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_del")
	return c.Client.Del(keys...)
}

func (c RedisClient) Unlink(key ...string) *redis.IntCmd {
	//redis support unlink from 4.0
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_unlink")
	cmd := c.Client.Unlink(key...)
	if cmd.Err() != nil {
		return c.Del(key...)
	}

	return cmd
}

func (c RedisClient) Incr(key string) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_incr")
	return c.Client.Incr(key)
}

func (c RedisClient) SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_setnx")
	return c.Client.SetNX(key, value, expiration)
}

func (c RedisClient) Expire(key string, expiration time.Duration) *redis.BoolCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_expire")
	return c.Client.Expire(key, expiration)
}

func (c RedisClient) ExpireAt(key string, tm time.Time) *redis.BoolCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_expireAt")
	return c.Client.ExpireAt(key, tm)
}

func (c RedisClient) Rename(key, newkey string) *redis.StatusCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_rename")
	return c.Client.Rename(key, newkey)
}

//------------------------------------Hash------------------------------------------

func (c RedisClient) HSet(key, field string, value interface{}) *redis.BoolCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_hset")
	return c.Client.HSet(key, field, value)
}

func (c RedisClient) HGet(key, field string) *redis.StringCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_hget")
	return c.Client.HGet(key, field)
}

func (c RedisClient) HDel(key string, fields ...string) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_hdel")
	return c.Client.HDel(key, fields...)
}

func (c RedisClient) HExists(key, field string) *redis.BoolCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_hexists")
	return c.Client.HExists(key, field)
}

func (c RedisClient) HGetAll(key string) *redis.StringStringMapCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_hgetall")
	return c.Client.HGetAll(key)
}

func (c RedisClient) HIncrBy(key, field string, incr int64) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_hincrby")
	return c.Client.HIncrBy(key, field, incr)
}

func (c RedisClient) HMSet(key string, fields map[string]interface{}) *redis.StatusCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_hmset")
	return c.Client.HMSet(key, fields)
}

func (c RedisClient) HMGet(key string, fields ...string) *redis.SliceCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_hmget")
	return c.Client.HMGet(key, fields...)
}

func (c RedisClient) HSetNX(key, field string, value interface{}) *redis.BoolCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_hsetnx")
	return c.Client.HSetNX(key, field, value)
}

func (c RedisClient) HScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_hscan")
	return c.Client.HScan(key, cursor, match, count)
}

//------------------------------------Set-------------------------------------------

func (c RedisClient) SAdd(key string, members ...interface{}) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_sadd")
	return c.Client.SAdd(key, members...)
}

func (c RedisClient) SCard(key string) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_scard")
	return c.Client.SCard(key)
}

func (c RedisClient) SIsMember(key string, member interface{}) *redis.BoolCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_sismember")
	return c.Client.SIsMember(key, member)
}

func (c RedisClient) SMembers(key string) *redis.StringSliceCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_smembers")
	return c.Client.SMembers(key)
}

func (c RedisClient) SRem(key string, members ...interface{}) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_srem")
	return c.Client.SRem(key, members...)
}

//------------------------------------ZSet------------------------------------------

func (c RedisClient) ZAdd(key string, members ...redis.Z) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_zadd")
	return c.Client.ZAdd(key, members...)
}

func (c RedisClient) ZScore(key, member string) *redis.FloatCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_zscore")
	return c.Client.ZScore(key, member)
}

func (c RedisClient) ZAddXX(key string, members ...redis.Z) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_zaddxx")
	return c.Client.ZAddXX(key, members...)
}

func (c RedisClient) ZAddNX(key string, members ...redis.Z) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_zaddnx")
	return c.Client.ZAddNX(key, members...)
}

func (c RedisClient) ZCard(key string) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_zcard")
	return c.Client.ZCard(key)
}

func (c RedisClient) ZRem(key string, members ...interface{}) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_zrem")
	return c.Client.ZRem(key, members...)
}

func (c RedisClient) ZCount(key, min, max string) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_zcount")
	return c.Client.ZCount(key, min, max)
}

//ZIncr redis support zadd for option from 3.0.2 ,equal ZAdd if member.Member not exist
func (c RedisClient) ZIncr(key string, member redis.Z) *redis.FloatCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_zincr")
	cmd := c.Client.ZIncr(key, member)
	if cmd.Err() != nil {
		return c.ZIncrBy(key, member.Score, member.Member.(string))
	}
	return cmd
}

func (c RedisClient) ZIncrBy(key string, increment float64, member string) *redis.FloatCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_zincrby")
	return c.Client.ZIncrBy(key, increment, member)
}

//ZRankX 返回正序 or 倒序
func (c RedisClient) ZRankX(key, member string, rev bool) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_rank")
	if rev {
		return c.Client.ZRevRank(key, member) //返回member排名
	} else {
		return c.Client.ZRank(key, member) //返回索引
	}
}

//ZRangeWithScoresX 返回正序 or 倒序的名次范围的zSet
func (c RedisClient) ZRangeWithScoresX(key string, start, stop int64, rev bool) *redis.ZSliceCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_range_with_score")
	if rev {
		return c.Client.ZRevRangeWithScores(key, start, stop)
	} else {
		return c.Client.ZRangeWithScores(key, start, stop)
	}
}

//ZRangeX 返回正序 or 倒序的名次范围的zSet.Member
func (c RedisClient) ZRangeX(key string, start, stop int64, rev bool) *redis.StringSliceCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_range")
	if rev {
		return c.Client.ZRevRange(key, start, stop)
	} else {
		return c.Client.ZRange(key, start, stop)
	}
}

//ZRemRangeByRank 根据倒序排名移出
func (c RedisClient) ZRemRangeByRank(key string, start, stop int64) *redis.IntCmd {
	//defer monitor.TimeIt(time.Now(), monitor.TotalRedisMetric.RedisMetric(c.clientName).AddMetric, "time_zrem_range_by_rank")
	return c.Client.ZRemRangeByRank(key, start, stop)
}

//------------------------------------List------------------------------------------

func (c RedisClient) LPush(key string, values ...interface{}) *redis.IntCmd {
	return c.Client.LPush(key, values...)
}

func (c RedisClient) RPush(key string, values ...interface{}) *redis.IntCmd {
	return c.Client.RPush(key, values...)
}

func (c RedisClient) LLen(key string) *redis.IntCmd {
	return c.Client.LLen(key)
}

func (c RedisClient) LPop(key string) *redis.StringCmd {
	return c.Client.LPop(key)
}

func (c RedisClient) RPop(key string) *redis.StringCmd {
	return c.Client.RPop(key)
}

func (c RedisClient) LRem(key string, count int64, value interface{}) *redis.IntCmd {
	return c.Client.LRem(key, count, value)
}

func (c RedisClient) LIndex(key string, index int64) *redis.StringCmd {
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
