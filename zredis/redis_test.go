package zredis

import (
	"fmt"
	"github.com/go-redis/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
	"testing"
	"time"
)

const (
	ADDR = "localhost"
	PORT = "6379"
)

func TestRedis(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Redis")
}

var _ = Describe("Test Redis", func() {

	var client *RedisClient

	//测试前初始化redis客户端,并清空!!!
	BeforeEach(func() {
		opt := redis.Options{
			Addr:               ADDR + ":" + PORT,
			DB:                 15,
			DialTimeout:        10 * time.Second,
			ReadTimeout:        30 * time.Second,
			WriteTimeout:       30 * time.Second,
			PoolSize:           1,
			PoolTimeout:        30 * time.Second,
			IdleTimeout:        500 * time.Millisecond,
			IdleCheckFrequency: 500 * time.Millisecond,
			MinIdleConns:       0,
			MaxConnAge:         0,
		}
		client = NewClient(&opt, "test")
		// HaveOccurred will success if err is non-nil so HaveOccurred Test not success
		Expect(client.FlushDB().Err()).ShouldNot(HaveOccurred())
	})

	//测试后关闭redis客户端
	AfterEach(func() {
		Expect(client.Close()).ShouldNot(HaveOccurred())
	})

	Context("Test Public", func() {

		It("Test Keys", func() {
			for i := 1; i <= 10; i++ {
				set, err := client.Set(fmt.Sprintf("key%d", i), i, 0).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(set).Should(Equal("OK"))
			}

			keys, err := client.Keys("key?").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(keys).Should(ContainElements("key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9"))

			keys, err = client.Keys("*1*").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(keys).Should(ContainElements("key1", "key10"))
		})

		It("Test Scan", func() {
			for i := 0; i < 1000; i++ {
				set, err := client.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("hello%d", i), 0).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(set).Should(Equal("OK"))
			}
			keys, cursor, err := client.Scan(0, "", 50).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(keys).ShouldNot(BeEmpty())
			Expect(cursor).ShouldNot(BeZero())

			keys, cursor, err = client.Scan(cursor, "", 950).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(keys).ShouldNot(BeEmpty())
			Expect(cursor).Should(BeZero())
		})

		It("Test PubSub", func() {
			pubsub := client.Subscribe("c1", "c2")
			sub := pubsub.Channel()
			defer pubsub.Close()

			go func() {
				defer GinkgoRecover()
				pub1, err := client.Publish("c1", "hello").Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(pub1).Should(Equal(int64(1)))

				time.Sleep(10 * time.Millisecond)
				pub2, err := client.Publish("c2", "world").Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(pub2).Should(Equal(int64(1)))
			}()

			var msg *redis.Message
			Eventually(sub).Should(Receive(&msg))
			Expect(msg.String()).Should(Equal("Message<c1: hello>"))
			Eventually(sub).Should(Receive(&msg))
			Expect(msg.String()).Should(Equal("Message<c2: world>"))
		})

		It("Test PipeLine", func() {
			pipe := client.Pipeline()
			set, err := pipe.Set("a", "value", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(set).Should(BeEmpty())

			get, err := pipe.Get("a").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(get).Should(BeEmpty())

			cmd, err := pipe.Exec()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len(cmd)).Should(Equal(2))

			setCommand := ""
			statusCmd := cmd[0].(*redis.StatusCmd)
			for _, command := range statusCmd.Args() {
				setCommand += " " + command.(string)
			}
			Expect(setCommand[1:]).Should(Equal("set a value"))
			Expect(statusCmd.Val()).Should(Equal("OK"))

			getCommand := ""
			stringCmd := cmd[1].(*redis.StringCmd)
			for _, command := range stringCmd.Args() {
				getCommand += " " + command.(string)
			}
			Expect(getCommand[1:]).Should(Equal("get a"))
			Expect(stringCmd.Val()).Should(Equal("value"))
		})
	})

	Context("Test Key", func() {

		It("Test Set And Get", func() {
			set, err := client.Set("key", "hello", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(set).Should(Equal("OK"))

			get, err := client.Get("key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(get).Should(Equal("hello"))
		})

		It("Test Del", func() {
			set1, err := client.Set("key1", "v1", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(set1).Should(Equal("OK"))

			set2, err := client.Set("key2", "v2", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(set2).Should(Equal("OK"))

			del, err := client.Del("key1", "key2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(del).Should(Equal(int64(2)))
		})

		It("Test Unlink", func() {
			set1, err := client.Set("key1", "v1", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(set1).Should(Equal("OK"))

			set2, err := client.Set("key2", "v2", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(set2).Should(Equal("OK"))

			unlink, err := client.Unlink("key1", "key2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(unlink).Should(Equal(int64(2)))
		})

		It("Test Incr", func() {
			set, err := client.Set("key", 10, 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(set).Should(Equal("OK"))

			incr, err := client.Incr("key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(incr).Should(Equal(int64(11)))

			get, err := client.Get("key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(get).Should(Equal("11"))
		})

		It("Test SetNX", func() {
			setNX, err := client.SetNX("key", "v1", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(setNX).Should(BeTrue())

			get, err := client.Get("key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(get).Should(Equal("v1"))

			setNX, err = client.SetNX("key", "v2", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(setNX).Should(BeFalse())

			get, err = client.Get("key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(get).Should(Equal("v1"))
		})

		It("Test Expire", func() {
			set, err := client.Set("key", "v1", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(set).Should(Equal("OK"))

			ttl, err := client.TTL("key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ttl < 0).Should(BeTrue())

			expire, err := client.Expire("key", 10*time.Second).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(expire).Should(BeTrue())

			ttl, err = client.TTL("key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ttl).Should(Equal(10 * time.Second))
		})

		It("Test ExpireAt", func() {
			set, err := client.Set("key", "v1", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(set).Should(Equal("OK"))

			exists, err := client.Exists("key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(exists).Should(Equal(int64(1)))

			expireAt, err := client.ExpireAt("key", time.Now().Add(-time.Hour)).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(expireAt).Should(BeTrue())

			exists, err = client.Exists("key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(exists).Should(Equal(int64(0)))
		})

		It("Test Rename", func() {
			set, err := client.Set("key1", "v", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(set).Should(Equal("OK"))

			exists, err := client.Exists("key1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(exists).Should(Equal(int64(1)))

			rename, err := client.Rename("key1", "key2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rename).Should(Equal("OK"))

			exists, err = client.Exists("key1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(exists).Should(Equal(int64(0)))

			exists, err = client.Exists("key2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(exists).Should(Equal(int64(1)))
		})
	})

	Context("Test Hash", func() {

		It("Test HSet And HGet", func() {
			hSet, err := client.HSet("hash", "key", "hello").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hSet).Should(BeTrue())

			hGet, err := client.HGet("hash", "key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hGet).Should(Equal("hello"))

			hGet, err = client.HGet("hash", "key1").Result()
			Expect(err).Should(Equal(redis.Nil))
			Expect(hGet).Should(Equal(""))
		})

		It("Test HDel", func() {
			hSet, err := client.HSet("hash", "key", "hello").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hSet).Should(BeTrue())

			hDel, err := client.HDel("hash", "key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hDel).Should(Equal(int64(1)))

			hDel, err = client.HDel("hash", "key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hDel).Should(Equal(int64(0)))
		})

		It("Test HExists", func() {
			hSet, err := client.HSet("hash", "key", "hello").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hSet).Should(BeTrue())

			hExists, err := client.HExists("hash", "key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hExists).Should(BeTrue())

			hExists, err = client.HExists("hash", "key1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hExists).Should(BeFalse())
		})

		It("Test HGetAll", func() {
			hSet, err := client.HSet("hash", "key1", "hello1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hSet).Should(BeTrue())

			hSet, err = client.HSet("hash", "key2", "hello2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hSet).Should(BeTrue())

			m, err := client.HGetAll("hash").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).Should(Equal(map[string]string{"key1": "hello1", "key2": "hello2"}))
		})

		It("Test HIncrBy", func() {
			hSet, err := client.HSet("hash", "key", "5").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hSet).Should(BeTrue())

			hIncrBy, err := client.HIncrBy("hash", "key", 1).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hIncrBy).Should(Equal(int64(6)))

			hIncrBy, err = client.HIncrBy("hash", "key", -1).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hIncrBy).Should(Equal(int64(5)))

			hIncrBy, err = client.HIncrBy("hash", "key", -10).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hIncrBy).Should(Equal(int64(-5)))
		})

		It("Test HMSet", func() {
			hmSet, err := client.HMSet("hash", map[string]interface{}{
				"key1": "hello1",
				"key2": "hello2",
			}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hmSet).Should(Equal("OK"))

			hGet, err := client.HGet("hash", "key1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hGet).Should(Equal("hello1"))

			hGet, err = client.HGet("hash", "key2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hGet).Should(Equal("hello2"))
		})

		It("Test HMGet", func() {
			hashMap := map[string]interface{}{
				"key1": "hello1",
				"key2": "hello2",
			}

			hmSet, err := client.HMSet("hash", hashMap).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hmSet).Should(Equal("OK"))

			hmGet, err := client.HMGet("hash", "key1", "key", "key2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hmGet).Should(Equal([]interface{}{hashMap["key1"], nil, hashMap["key2"]}))
		})

		It("Test HSetNX", func() {
			hSetNX, err := client.HSetNX("hash", "key", "hello").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hSetNX).Should(BeTrue())

			hSetNX, err = client.HSetNX("hash", "key", "hello1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hSetNX).Should(BeFalse())

			hGet, err := client.HGet("hash", "key").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(hGet).Should(Equal("hello"))
		})

		It("Test HScan", func() {
			for i := 0; i < 1000; i++ {
				hSet, err := client.HSet("hash", fmt.Sprintf("key%d", i), fmt.Sprintf("hello%d", i)).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(hSet).Should(BeTrue())
			}

			keys, cursor, err := client.HScan("hash", 0, "", 50).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(keys).ShouldNot(BeEmpty())
			Expect(cursor).ShouldNot(BeZero())

			keys, cursor, err = client.HScan("hash", cursor, "", 950).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(keys).ShouldNot(BeEmpty())
			Expect(cursor).Should(BeZero())
		})
	})

	Context("Test Set", func() {

		It("Test SAdd", func() {
			sAdd, err := client.SAdd("set", 1).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sAdd).Should(Equal(int64(1)))

			sAdd, err = client.SAdd("set", 1).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sAdd).Should(Equal(int64(0)))

			sAdd, err = client.SAdd("set", 2).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sAdd).Should(Equal(int64(1)))
		})

		It("Test SCard", func() {
			for i := 0; i < 10; i++ {
				sAdd, err := client.SAdd("set", i).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(sAdd).Should(Equal(int64(1)))
			}

			sCard, err := client.SCard("set").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sCard).Should(Equal(int64(10)))
		})

		It("Test SIsMember", func() {
			sAdd, err := client.SAdd("set", "1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sAdd).Should(Equal(int64(1)))

			sIsMember, err := client.SIsMember("set", "1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sIsMember).Should(BeTrue())

			sIsMember, err = client.SIsMember("set", "2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sIsMember).Should(BeFalse())

			sIsMember, err = client.SIsMember("", "1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sIsMember).Should(BeFalse())
		})

		It("Test SMembers", func() {
			for i := 0; i < 10; i++ {
				sAdd, err := client.SAdd("set", i).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(sAdd).Should(Equal(int64(1)))
			}

			sMembers, err := client.SMembers("set").Result()
			Expect(err).ShouldNot(HaveOccurred())
			for i := 0; i < 10; i++ {
				Expect(sMembers[i]).Should(Equal(strconv.Itoa(i)))
			}
		})

		It("Test SRem", func() {
			for i := 0; i < 10; i++ {
				sAdd, err := client.SAdd("set", i).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(sAdd).Should(Equal(int64(1)))
			}

			sIsMember, err := client.SIsMember("set", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sIsMember).Should(BeTrue())

			sRem, err := client.SRem("set", 0, 1, 2).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sRem).Should(Equal(int64(3)))

			sIsMember, err = client.SIsMember("set", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(sIsMember).Should(BeFalse())
		})
	})

	Context("Test ZSet", func() {

		It("Test ZAdd", func() {
			zAdd, err := client.ZAdd("zSet", redis.Z{Score: 1, Member: "one"}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAdd).Should(Equal(int64(1)))

			zAdd, err = client.ZAdd("zSet", redis.Z{Score: 1, Member: "=.="}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAdd).Should(Equal(int64(1)))

			zAdd, err = client.ZAdd("zSet", redis.Z{Score: 2, Member: "=.="}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAdd).Should(Equal(int64(0)))
		})

		It("Test ZScore", func() {
			zAdd, err := client.ZAdd("zSet", redis.Z{Score: 520, Member: "=.="}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAdd).Should(Equal(int64(1)))

			zScore, err := client.ZScore("zSet", "=.=").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zScore).Should(Equal(float64(520)))
		})

		It("Test ZAddXX", func() {
			zAddXX, err := client.ZAddXX("zSet", redis.Z{Score: 1, Member: "one"}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAddXX).Should(Equal(int64(0)))

			zScore, err := client.ZScore("zSet", "one").Result()
			Expect(err).Should(HaveOccurred())
			Expect(zScore).Should(Equal(float64(0)))

			zAdd, err := client.ZAdd("zSet", redis.Z{Score: 1, Member: "=.="}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAdd).Should(Equal(int64(1)))

			zScore, err = client.ZScore("zSet", "=.=").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zScore).Should(Equal(float64(1)))

			zAddXX, err = client.ZAddXX("zSet", redis.Z{Score: 2, Member: "=.="}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAddXX).Should(Equal(int64(0)))

			zScore, err = client.ZScore("zSet", "=.=").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zScore).Should(Equal(float64(2)))
		})

		It("Test ZAddNX", func() {
			zAddNX, err := client.ZAddNX("zSet", redis.Z{Score: 1, Member: "=.="}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAddNX).Should(Equal(int64(1)))

			zScore, err := client.ZScore("zSet", "=.=").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zScore).Should(Equal(float64(1)))

			zAddNX, err = client.ZAddNX("zSet", redis.Z{Score: 2, Member: "=.="}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAddNX).Should(Equal(int64(0)))

			zScore, err = client.ZScore("zSet", "=.=").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zScore).Should(Equal(float64(1)))
		})

		It("Test ZCard", func() {
			for i := 0; i < 10; i++ {
				zAdd, err := client.ZAdd("zSet", redis.Z{Score: float64(-i), Member: i}).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(zAdd).Should(Equal(int64(1)))
			}

			zCard, err := client.ZCard("zSet").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zCard).Should(Equal(int64(10)))
		})

		It("Test ZRem", func() {
			for i := 0; i < 10; i++ {
				zAdd, err := client.ZAdd("zSet", redis.Z{Score: float64(-i), Member: i}).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(zAdd).Should(Equal(int64(1)))
			}

			zRem, err := client.ZRem("zSet", 1, 2).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zRem).Should(Equal(int64(2)))

			zScore, err := client.ZScore("zSet", "1").Result()
			Expect(err).Should(HaveOccurred())
			Expect(zScore).Should(Equal(float64(0)))

			zScore, err = client.ZScore("zSet", "2").Result()
			Expect(err).Should(HaveOccurred())
			Expect(zScore).Should(Equal(float64(0)))

			zScore, err = client.ZScore("zSet", "3").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zScore).Should(Equal(float64(-3)))
		})

		It("Test ZCount", func() {
			for i := 0; i < 10; i++ {
				zAdd, err := client.ZAdd("zSet", redis.Z{Score: float64(-i), Member: i}).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(zAdd).Should(Equal(int64(1)))
			}

			zCount, err := client.ZCount("zSet", "-inf", "+inf").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zCount).Should(Equal(int64(10)))

			zCount, err = client.ZCount("zSet", "-5", "5").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zCount).Should(Equal(int64(6)))

			zCount, err = client.ZCount("zSet", "(-5", "5").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zCount).Should(Equal(int64(5)))
		})

		It("Test ZIncr", func() {
			zAdd, err := client.ZAdd("zSet", redis.Z{Score: -1, Member: 1}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAdd).Should(Equal(int64(1)))

			zIncr, err := client.ZIncr("zSet", redis.Z{Score: -8, Member: "1"}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zIncr).Should(Equal(float64(-9)))
		})

		It("Test ZIncrBy", func() {
			zAdd, err := client.ZAdd("zSet", redis.Z{Score: -1, Member: 1}).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zAdd).Should(Equal(int64(1)))

			zIncrBy, err := client.ZIncrBy("zSet", -8, "1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zIncrBy).Should(Equal(float64(-9)))
		})

		It("Test ZRankX", func() {
			for i := 0; i < 10; i++ {
				zAdd, err := client.ZAdd("zSet", redis.Z{Score: float64(-i), Member: i}).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(zAdd).Should(Equal(int64(1)))
			}

			zRankXRev, err := client.ZRankX("zSet", "1", true).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zRankXRev).Should(Equal(int64(1)))

			zRankX, err := client.ZRankX("zSet", "9", false).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zRankX).Should(Equal(int64(0)))
		})

		It("Test ZRangeWithScoresX", func() {
			for i := 0; i < 10; i++ {
				zAdd, err := client.ZAdd("zSet", redis.Z{Score: float64(-i), Member: i}).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(zAdd).Should(Equal(int64(1)))
			}

			zRevRangeWithScoresX, err := client.ZRangeWithScoresX("zSet", 0, 2, true).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zRevRangeWithScoresX).Should(Equal([]redis.Z{{
				Score:  0,
				Member: "0",
			}, {
				Score:  -1,
				Member: "1",
			}, {
				Score:  -2,
				Member: "2",
			}}))

			zRangeWithScoresX, err := client.ZRangeWithScoresX("zSet", -3, -1, false).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zRangeWithScoresX).Should(Equal([]redis.Z{{
				Score:  -2,
				Member: "2",
			}, {
				Score:  -1,
				Member: "1",
			}, {
				Score:  0,
				Member: "0",
			}}))
		})

		It("Test ZRangeX", func() {
			for i := 0; i < 10; i++ {
				zAdd, err := client.ZAdd("zSet", redis.Z{Score: float64(-i), Member: i}).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(zAdd).Should(Equal(int64(1)))
			}

			zRevRangeX, err := client.ZRangeX("zSet", 0, 2, true).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zRevRangeX).Should(Equal([]string{"0", "1", "2"}))

			zRangeX, err := client.ZRangeX("zSet", -3, -1, false).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zRangeX).Should(Equal([]string{"2", "1", "0"}))
		})

		It("Test ZRemRangeByRank", func() {
			for i := 0; i < 10; i++ {
				zAdd, err := client.ZAdd("zSet", redis.Z{Score: float64(-i), Member: i}).Result()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(zAdd).Should(Equal(int64(1)))
			}

			zRevRangeX, err := client.ZRangeX("zSet", 0, 5, false).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zRevRangeX).Should(Equal([]string{"9", "8", "7", "6", "5", "4"}))

			zRemRangeByRank, err := client.ZRemRangeByRank("zSet", 1, 3).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zRemRangeByRank).Should(Equal(int64(3)))

			zRevRangeX, err = client.ZRangeX("zSet", 0, 5, false).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(zRevRangeX).Should(Equal([]string{"9", "5", "4", "3", "2", "1"}))
		})
	})

	Context("Test List", func() {

		It("Text LPush", func() {
			lPush, err := client.Client.LPush("list", 1).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(1)))
		})

		It("Text RPush", func() {
			rPush, err := client.RPush("list", 1).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rPush).Should(Equal(int64(1)))
		})

		It("Text LLen", func() {
			lPush, err := client.LPush("list", 1).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(1)))

			len, err := client.LLen("list").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len).Should(Equal(int64(1)))

			rPush, err := client.RPush("list", 2).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rPush).Should(Equal(int64(2)))

			len, err = client.LLen("list").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len).Should(Equal(int64(2)))
		})

		It("Text LPop", func() {
			lPush, err := client.LPush("list", "l1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(1)))

			rPush, err := client.RPush("list", "r1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rPush).Should(Equal(int64(2)))

			lPop, err := client.LPop("list").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPop).Should(Equal("l1"))

			lPush, err = client.LPush("list", "l2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(2)))

			rPush, err = client.RPush("list", "r2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rPush).Should(Equal(int64(3)))

			lPop, err = client.LPop("list").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPop).Should(Equal("l2"))
		})

		It("Text RPop", func() {
			lPush, err := client.LPush("list", "l1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(1)))

			rPush, err := client.RPush("list", "r1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rPush).Should(Equal(int64(2)))

			rPop, err := client.RPop("list").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rPop).Should(Equal("r1"))

			lPush, err = client.LPush("list", "l2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(2)))

			rPush, err = client.RPush("list", "r2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rPush).Should(Equal(int64(3)))

			rPop, err = client.RPop("list").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rPop).Should(Equal("r2"))
		})

		It("Text LRem", func() {
			lPush, err := client.LPush("list", "l1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(1)))

			lPush, err = client.LPush("list", "l2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(2)))

			lPush, err = client.LPush("list", "l1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(3)))

			lRem, err := client.LRem("list", 1, "l1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lRem).Should(Equal(int64(1)))

			lPop, err := client.LPop("list").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPop).Should(Equal("l2"))
		})

		It("Text LIndex", func() {
			lPush, err := client.LPush("list", "l1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(1)))

			lPush, err = client.LPush("list", "l2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(2)))

			lPush, err = client.RPush("list", "r1").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(3)))

			lPush, err = client.RPush("list", "r2").Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lPush).Should(Equal(int64(4)))

			lIndex, err := client.LIndex("list", 0).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lIndex).Should(Equal("l2"))

			lIndex, err = client.LIndex("list", -1).Result()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(lIndex).Should(Equal("r2"))
		})
	})
})
