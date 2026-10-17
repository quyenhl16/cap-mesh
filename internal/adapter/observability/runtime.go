package observability

import (
	"net/http"
	pprofhandler "net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func RegisterRuntimeCollectors(registerer prometheus.Registerer) {
	registerer.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
}

func NewPprofServer(address string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprofhandler.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprofhandler.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprofhandler.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprofhandler.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprofhandler.Trace)
	for _, profile := range []string{"allocs", "block", "goroutine", "heap", "mutex", "threadcreate"} {
		mux.Handle("/debug/pprof/"+profile, pprofhandler.Handler(profile))
	}
	return &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}
