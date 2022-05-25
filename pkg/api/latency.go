package api

import (
	"github.com/stefanprodan/podinfo/pkg/fscache"
	"go.uber.org/zap"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type LatencyMiddleware struct {
	logger  *zap.Logger
	watcher *fscache.Watcher
}

func (l *LatencyMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		latency, err := strconv.Atoi(latencyString)
		if err != nil {
			l.logger.Error("failed to convert latency to int")
			return
		}

		jitterRange := float64(latency) * 0.2 // 20 percent of jitter
		jitter := rand.Intn(int(jitterRange))

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
	rand.Seed(time.Now().Unix())

	return &LatencyMiddleware{
		logger:  logger,
		watcher: w,
	}, nil
}
