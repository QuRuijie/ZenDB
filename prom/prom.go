package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"runtime"
	"strings"
	"time"
)

var (
	//------------------------mongo metrics------------------------
	mongoRequestDuration = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name: "mongo_request_duration",
		Help: "The duration of processed mongo requests",
	}, []string{"method", "status"})

	mongoRequestCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mongo_request_count",
		Help: "The count of processed mongo requests",
	}, []string{"method", "status"})

	//------------------------redis  metrics------------------------
	redisRequestDuration = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name: "redis_request_duration",
		Help: "The duration of processed redis requests",
	}, []string{"method", "status"})

	redisRequestCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_request_count",
		Help: "The count of processed redis requests",
	}, []string{"method", "status"})
)

// StartServer 开启服务器, 等待prometheus拉取指标
func StartServer(addr string) (err error) {
	http.Handle("/metrics", promhttp.Handler())
	if addr[0] != ':' {
		addr = ":" + addr
	}
	err = http.ListenAndServe(addr, nil)
	return
}

// SetMongoMetrics 设置mongo指标
func SetMongoMetrics(start int64, err error) {
	method := runFuncName(4)
	status := "Success"
	if err != nil {
		status = "Fail"
	}
	mongoRequestDuration.WithLabelValues(method, status).Observe((float64)(NowMillisecond() - start))
	mongoRequestCount.WithLabelValues(method, status)
}

// SetRedisMetrics 设置redis指标
func SetRedisMetrics(start int64, err error) {
	method := runFuncName(4)
	status := "Success"
	if err != nil {
		status = "Fail"
	}
	redisRequestDuration.WithLabelValues(method, status).Observe((float64)(NowMillisecond() - start))
	redisRequestCount.WithLabelValues(method, status)
}

// NowMillisecond 获取当前的时间戳(微秒)
func NowMillisecond() (now int64) {
	return time.Now().UnixNano() / int64(time.Microsecond)
}

// 获取正在运行的函数的"上*num"个函数名,例如3,main()->SetRedisMetrics()->runFuncName(),输出的则是"main"
func runFuncName(num int) string {
	pc := make([]uintptr, 1)
	runtime.Callers(num, pc)
	f := runtime.FuncForPC(pc[0])
	s := strings.Split(f.Name(), ".")
	return s[len(s)-1]
}
