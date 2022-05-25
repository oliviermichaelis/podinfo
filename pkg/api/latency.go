package api

import (
	"github.com/stefanprodan/podinfo/pkg/fscache"
	"go.uber.org/zap"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type LatencyMiddleware struct {
	logger  *zap.Logger
	watcher *fscache.Watcher
}

func (l *LatencyMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := rand.Intn(100)
		if i < 90 { // 90 percent are not affected
			next.ServeHTTP(w, r)
			return
		}

		v, ok := l.watcher.Cache.Load("latency")
		if !ok {
			l.logger.Error("unable to fetch /latency/latency file")
			return
		}

		latencyString, ok := v.(string)
		if !ok {
			l.logger.Error("failed to type assert latency file content to string")
			return
		}

		latency, err := strconv.Atoi(strings.TrimSpace(latencyString))
		if err != nil {
			l.logger.Error("failed to convert latency to int")
			return
		}

		jitterRange := float64(latency) * 0.2 // 20 percent of positive jitter
		jitter := rand.Intn(int(math.Ceil(jitterRange)))

		time.Sleep(time.Duration(latency+jitter) * time.Millisecond)
		next.ServeHTTP(w, r)
	})
}

func NewLatencyMiddleware(logger *zap.Logger) (*LatencyMiddleware, error) {
	w, err := fscache.NewWatch("/latency")
	if err != nil {
		return nil, err
	}

	w.Watch()
	logger.Info("started to watch /latency")
	rand.Seed(time.Now().Unix())

	return &LatencyMiddleware{
		logger:  logger,
		watcher: w,
	}, nil
}
