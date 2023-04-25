package api

import (
	"encoding/json"
	"fmt"
	"gonum.org/v1/gonum/stat/distuv"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

var (
	Histogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Subsystem: "faults",
		Name:      "latency",
	})
)

func init() {
	prometheus.MustRegister(Histogram)
}

type Quantiles struct {
	SuccessRate float64 `json:"success_rate"`
	P50         int     `json:"p50"`
	P99         int     `json:"p99"`
}

type LatencyMiddleware struct {
	sync.RWMutex
	logger       *zap.Logger
	successRate  float64
	distribution distuv.LogNormal
}

func (l *LatencyMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip all requests not coming from the load generator
		if r.RequestURI != "/" {
			next.ServeHTTP(w, r)
			return
		}

		l.RLock()
		latency := l.distribution.Rand()
		success := l.successRate
		l.RUnlock()

		// Cap the latency to 7000ms
		if latency > 7000 {
			latency = 7000
		}

		// rand.Float64() takes a random number from a right-open interval
		if success != 1.0 {
			r := rand.Float64()
			if r > success {
				time.Sleep(time.Duration(latency) * time.Millisecond)
				w.WriteHeader(http.StatusInternalServerError)
				Histogram.Observe(latency)
				return
			}
		}

		Histogram.Observe(latency)
		time.Sleep(time.Duration(latency) * time.Millisecond)
		next.ServeHTTP(w, r)
	})
}

func (l *LatencyMiddleware) setDistribution(p50Latency, p99Latency float64) {
	p50 := normalCDFInverse(0.5)
	p99 := normalCDFInverse(0.99)

	l.distribution.Sigma = (math.Log(p99Latency) - math.Log(p50Latency)) / (p99 - p50)
	l.distribution.Mu = (math.Log(p50Latency)*p99 - math.Log(p99Latency)*p50) / (p99 - p50)
}

func (l *LatencyMiddleware) setSuccessRate(successRate float64) {
	l.successRate = successRate
}

func NewLatencyMiddleware(logger *zap.Logger, queueName, endpoint, username, password string) (*LatencyMiddleware, error) {
	if endpoint == "" {
		log.Fatal("endpoint is empty")
	}
	if username == "" {
		log.Fatal("username is empty")
	}
	if password == "" {
		log.Fatal("password is empty")
	}

	var scheme string
	if strings.HasPrefix(endpoint, "amqps://") {
		scheme = "amqps://"
	} else if strings.HasPrefix(endpoint, "amqp://") {
		scheme = "amqp://"
	} else {
		logger.Panic("unexpected queue scheme")
	}

	endpoint = strings.TrimPrefix(endpoint, scheme)
	address := fmt.Sprintf("%s%s:%s@%s", scheme, username, password, endpoint)
	conn, err := amqp.Dial(address)
	if err != nil {
		log.Fatal(err, zap.String("address", address))
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	if err := ch.ExchangeDeclare(queueName, amqp.ExchangeFanout, false, false, false, false, nil); err != nil {
		return nil, err
	}

	queue, err := ch.QueueDeclare("", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	if err = ch.QueueBind(queue.Name, "", queueName, false, nil); err != nil {
		return nil, err
	}

	delivery, err := ch.Consume(queue.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	l := &LatencyMiddleware{
		logger:       logger,
		distribution: distuv.LogNormal{},
	}
	l.setDistribution(1.0, 10.0)

	go func() {
		logger.Info("started to consume messages", zap.String("queue", queueName))
		for d := range delivery {
			var q Quantiles
			if err := json.Unmarshal(d.Body, &q); err != nil {
				log.Fatal(err)
			}

			l.Lock()
			l.setDistribution(float64(q.P50), float64(q.P99))
			l.setSuccessRate(q.SuccessRate)
			l.Unlock()
			logger.Info("received quantiles", zap.Int("P50", q.P50), zap.Int("P99", q.P99), zap.Float64("sigma", l.distribution.Sigma), zap.Float64("mu", l.distribution.Mu))
		}
	}()

	rand.Seed(time.Now().Unix())

	return l, nil
}

func rationalApproximation(t float64) float64 {
	// Abramowitz and Stegun formula 26.2.23.
	// The absolute value of the error should be less than 4.5 e-4.
	c := []float64{2.515517, 0.802853, 0.010328}
	d := []float64{1.432788, 0.189269, 0.001308}
	return t - ((c[2]*t+c[1])*t+c[0])/(((d[2]*t+d[1])*t+d[0])*t+1.0)
}

func normalCDFInverse(p float64) float64 {
	if p <= 0.0 || p >= 1.0 {
		panic("invalid argument")
	}

	if p < 0.5 {
		// F^-1(p) = - G^-1(p)
		return -rationalApproximation(math.Sqrt(-2.0 * math.Log(p)))
	}
	// F^-1(p) = G^-1(1-p)
	return rationalApproximation(math.Sqrt(-2.0 * math.Log(1-p)))
}
