package main

import (
	"fmt"
	"github.com/QuRuijie/zenDB/prom"
	"github.com/QuRuijie/zenDB/zmgo"
	"github.com/QuRuijie/zenDB/zredis"
	"github.com/go-redis/redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

var gCommonClient *zmgo.MongoClient
var gProjectClientMap = make(map[string]*zmgo.MongoClient)

const (
	URI  = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	KEY  = "default"
	ADDR = "127.0.0.1:6379"
)

var (
	Mongo *zmgo.MongoClient
)

func recordMetrics() {
	go func() {
		for {
			opsProcessed.Inc()
			time.Sleep(2 * time.Second)
		}
	}()
}

var (
	opsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "myapp_processed_ops_total",
		Help: "The total number of processed events",
	})
)

func main() {
	//TestRedis()
	TestMongo()
}

func TestMongo() {
	c, err := zmgo.NewMongoClientWithProm("mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb")
	if err != nil {
		fmt.Println(err)
		return
	}

	c.InsertOne("test", "test_defer", bson.M{"a": "a"})
	var result interface{}
	fmt.Println("FindAll Start")
	err = c.FindOne(&result, "test", "test_defer", bson.M{})
	fmt.Println("FindAll End ", err)
}

func TestRedis() {
	//recordMetrics()
	go prom.StartServer("8888")
	time.Sleep(1 * time.Second)
	fmt.Println("/?")

	opt := redis.Options{
		Addr:               ADDR,
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
	client := zredis.NewClient(&opt, "test")
	client.Set("k", "v", 0)
	client.Get("k")

	for true {

	}
}
