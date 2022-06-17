package api

import (
	"encoding/json"
	"log"
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
	Gauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Subsystem: "faults",
		Name:      "latency",
	})
)

func init() {
	prometheus.MustRegister(Gauge)
}

type LatencyMiddleware struct {
	sync.RWMutex
	logger  *zap.Logger
	latency float64
}

func (l *LatencyMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := rand.Intn(100)
		if i < 90 { // 90 percent are not affected
			next.ServeHTTP(w, r)
			return
		}

		l.RLock()
		defer l.RUnlock()

		latency := l.latency
		Gauge.Set(latency)
		time.Sleep(time.Duration(latency) * time.Millisecond)
		next.ServeHTTP(w, r)
	})
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
		logger: logger,
	}

	go func() {
		logger.Info("started to consume messages", zap.String("queue", queueName))
		for d := range delivery {
			type v struct {
				Value int `json:"value"`
			}

			var value v
			if err := json.Unmarshal(d.Body, &value); err != nil {
				log.Fatal(err)
			}

			l.Lock()
			l.latency = float64(value.Value)
			l.Unlock()
			logger.Info("received latency", zap.Int("latency", value.Value))
		}
	}()

	rand.Seed(time.Now().Unix())

	return l, nil
}
