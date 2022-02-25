package db

import (
	"github.com/Zentertain/zenlog"
	"github.com/go-redis/redis"
	"time"
)

// RedisOption represents the options for build a redis client.
type RedisOption struct {
	Addr     string
	Pwd      string
	DB       int
	PoolSize int
}

// RedisClient represents a simple wrapper for redis.Client.
type RedisClient struct {
	*redis.Client
}

// NewRedisClient returns redis client.
func NewRedisClient(opts *RedisOption) *RedisClient {
	raw := redis.NewClient(&redis.Options{
		Addr:     opts.Addr,
		Password: opts.Pwd,
		DB:       opts.DB,
		PoolSize: opts.PoolSize,
	})
	_, err := raw.Ping().Result()
	if err != nil {
		zenlog.Debug("redis connect %s error: %+v", opts.Addr, err)
	} else {
		zenlog.Debug("redis connect %s ok\n", opts.Addr)
	}

	rc := new(RedisClient)
	rc.Client = raw
	return rc
}

func SetJobMux(r *RedisClient, key, val string, expireTime time.Duration) bool {
	for i := 0; i < 10; i++ {
		isSet := r.SetNX(key, val, expireTime)
		if isSet.Val() {
			return true
		}
		// spin 10ms
		time.Sleep(time.Millisecond * 10)
	}
	return false
}

func ReleaseJobMux(r *RedisClient, key string) {
	r.Unlink(key)
}
