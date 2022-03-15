package main

import (
	"github.com/QuRuijie/zenDB/zmgo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"
)

var gCommonClient *zmgo.MongoClient
var gProjectClientMap = make(map[string]*zmgo.MongoClient)

const (
	URI  = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	KEY  = "default"
	ADDR = "127.0.0.1:6380"
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
	recordMetrics()

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
