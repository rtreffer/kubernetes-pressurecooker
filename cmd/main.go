package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/marvasgit/kubernetes-multicooker/pkg/config"
	"github.com/marvasgit/kubernetes-multicooker/pkg/multicooker"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	pressureThresholdExceededTotal,
	pressureRecoveredTotal,
	pressureThresholdExceeded,
	pressureEnabled prometheus.Gauge
	f config.StartupFlags
)

func init() {
	flag.StringVar(&f.KubeConfig, "kubeconfig", "", "file path to kubeconfig")
	flag.Float64Var(&f.TaintThreshold, "taint-threshold", 25, "pressure threshold value")
	flag.Float64Var(&f.EvictThreshold, "evict-threshold", 50, "pressure threshold value")
	flag.StringVar(&f.EvictBackoff, "evict-backoff", "10m", "time to wait between evicting Pods")
	flag.StringVar(&f.MinPodAge, "min-pod-age", "5m", "minimum age of Pods to be evicted")
	flag.StringVar(&f.NodeName, "node-name", "", "current node name")
	flag.IntVar(&f.MetricsPort, "metrics-port", 8080, "port for prometheus metrics endpoint")
	flag.IntVar(&f.TargetMetric, "target-metric", 3, "target metric to use / 10,60,300 for pressure; and 1,5,15 for avarage/")
	flag.BoolVar(&f.UseAvarage, "use-avarage", false, "use loadavg instead of proc/pressure/cpu")
	flag.Parse()

	prometheusNamespace := "multicooker"
	pressureThresholdExceeded = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: prometheusNamespace,
		Name:      "pressure_threshold_exceeded",
		Help:      "cpu pressure is currently above (1) or below (0) threshold",
	})
	pressureThresholdExceededTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: prometheusNamespace,
		Name:      "pressure_threshold_exceeded_total",
		Help:      "number of times the pressure threshold was exceeded",
	})
	pressureRecoveredTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: prometheusNamespace,
		Name:      "pressure_recovered_total",
		Help:      "number of times the pressure on the node recovered",
	})
	pressureEnabled = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: prometheusNamespace,
		Name:      "enabled",
		Help:      "multicooker is enabled (1) or disabled (0)",
	})
}

func main() {
	prometheus.MustRegister(pressureThresholdExceeded)
	prometheus.MustRegister(pressureThresholdExceededTotal)
	prometheus.MustRegister(pressureRecoveredTotal)
	prometheus.MustRegister(pressureEnabled)

	if f.NodeName == "" {
		panic("-node-name not set")
	}

	cfg, err := loadKubernetesConfig(f)
	if err != nil {
		panic(err)
	}

	c, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}

	w, err := multicooker.NewWatcher(f.TaintThreshold, f.NodeName)
	if err != nil {
		panic(err)
	}

	t, err := multicooker.NewTainter(c, f.NodeName)
	if err != nil {
		panic(err)
	}

	e, err := multicooker.NewEvicter(c, f.NodeName, f.EvictBackoff, f.MinPodAge)
	if err != nil {
		panic(err)
	}

	closeChan := make(chan struct{})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		s := <-sigChan

		glog.Infof("received signal %s", s)

		close(closeChan)
	}()

	go func() {
		http.HandleFunc("/-/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte("OK\n"))
		})
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", f.MetricsPort), nil)
	}()

	isTainted, err := t.IsNodeTainted()
	if err != nil {
		panic(err)
	}

	isDisabled, err := t.IsMulticookerDisabled()
	lastDisabledCheck := time.Now()
	if err != nil {
		panic(err)
	}

	w.SetAsHigh(isTainted)

	handlePrometheusMetrics(isDisabled, isTainted)

	exc, errs := w.Run(closeChan, f.UseAvarage, f.TargetMetric)
	for {
		select {
		case evt, ok := <-exc:
			if !ok {
				glog.Infof("Channel closed; stopping")
				return
			}

			if time.Now().Sub(lastDisabledCheck) > 1*time.Minute {
				if disabled, err := t.IsMulticookerDisabled(); err == nil {
					isDisabled = disabled
					if isDisabled {
						pressureEnabled.Set(0)
					} else {
						pressureEnabled.Set(1)
					}
				} else {
					glog.Errorf("could not check multicooker.enabled: %s", err.Error())
				}
				lastDisabledCheck = time.Now()
			}

			if isDisabled && isTainted {
				if err := t.UntaintNode(multicooker.PressureThresholdEvent{}); err != nil {
					glog.Errorf("error while untainting node: %s", err.Error())
				} else {
					isTainted = false
				}
			}

			if isDisabled {
				glog.Infof("multicooker disabled, pressure: %v", evt)
				continue
			}

			if evt.IsCurrentlyHigh {
				if isTainted && evt.MeticValue > f.EvictThreshold {
					if _, err := e.EvictPod(evt); err != nil {
						glog.Errorf("error while evicting pod: %s", err.Error())
					}
					continue
				}

				glog.Infof(evt.Message)

				if err := t.TaintNode(evt); err != nil {
					glog.Errorf("error while tainting node: %s", err.Error())
				} else {
					isTainted = true
					pressureThresholdExceeded.Set(1)
					pressureThresholdExceededTotal.Inc()
				}
			} else {

				if !isTainted {
					continue
				}

				glog.Infof(evt.Message)
				if err := t.UntaintNode(evt); err != nil {
					glog.Errorf("error while removing taint from node: %s", err.Error())
				} else {
					isTainted = false
					pressureThresholdExceeded.Set(0)
					pressureRecoveredTotal.Inc()
				}
			}
		case err := <-errs:
			glog.Errorf("error while polling for status updates: %s", err.Error())
		}
	}
}

func handlePrometheusMetrics(isDisabled bool, isTainted bool) {

	if isDisabled {
		pressureEnabled.Set(0)
	} else {
		pressureEnabled.Set(1)
	}

	if isTainted {
		pressureThresholdExceeded.Set(1)
	} else {
		pressureThresholdExceeded.Set(0)
	}
}

func loadKubernetesConfig(f config.StartupFlags) (*rest.Config, error) {
	if f.KubeConfig == "" {
		return rest.InClusterConfig()
	}

	return clientcmd.BuildConfigFromFlags("", f.KubeConfig)
}
