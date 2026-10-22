package platform

import (
	"net/http"
	"runtime/debug"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

// Access logs have an explicit allowlist. Never read/copy bodies, headers,
// query strings, response bodies or arbitrary panic values into request logs.
func SafeHTTPLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		response := &accessLogResponse{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if problem := recover(); problem != nil {
				if problem == http.ErrAbortHandler {
					panic(problem)
				}
				response.WriteHeader(http.StatusInternalServerError)
				logx.Errorw("http handler panic", logx.Field("route", routeLabel(r.URL.Path)), logx.Field("stack", string(debug.Stack())))
			}
			fields := []logx.LogField{
				logx.Field("method", r.Method), logx.Field("route", routeLabel(r.URL.Path)),
				logx.Field("status", response.status), logx.Field("durationMs", time.Since(start).Milliseconds()),
				logx.Field("requestId", RequestIDFromContext(r.Context())),
			}
			if response.status >= 500 {
				logx.WithContext(r.Context()).Errorw("http request", fields...)
			} else {
				logx.WithContext(r.Context()).Infow("http request", fields...)
			}
		}()
		next.ServeHTTP(response, r)
	})
}

type accessLogResponse struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *accessLogResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *accessLogResponse) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	if code >= 200 {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *accessLogResponse) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}
