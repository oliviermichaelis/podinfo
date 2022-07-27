package api

import (
	"encoding/json"
	"gonum.org/v1/gonum/stat/distuv"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const (
	queueAddress = "amqp://user:oVHPh0Rz1UF7cWK7@10.1.1.253:5672"
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
	P50 int `json:"p50"`
	P99 int `json:"p99"`
}

type LatencyMiddleware struct {
	sync.RWMutex
	logger       *zap.Logger
	latency      float64
	distribution distuv.LogNormal
}

func (l *LatencyMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.RLock()
		latency := l.distribution.Rand()
		l.RUnlock()

		// Cap the latency to 5000ms
		if latency > 5000 {
			latency = 5000
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

func NewLatencyMiddleware(logger *zap.Logger, queueName string) (*LatencyMiddleware, error) {
	conn, err := amqp.Dial(queueAddress)
	if err != nil {
		log.Fatal(err)
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
