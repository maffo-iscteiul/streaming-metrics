package prom_metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sirupsen/logrus"
)

type Prom_metrics struct {
	namespace_count           prometheus.Counter
	pulsar_processed_msg      prometheus.Counter
	pulsar_processed_msg_time prometheus.Summary
	filter_time               prometheus.Summary
	filtered_msg              *prometheus.CounterVec
	// number_of_metrics_per_msg prometheus.Summary
	push_time                prometheus.Summary
	monitors_ticks_generated *prometheus.CounterVec
	monitors_sent            *prometheus.CounterVec

	Number_of_namespaces              func(n int)
	Inc_number_processed_msg          func()
	Observe_processing_time           func(t time.Duration)
	Observe_filter_time               func(t time.Duration)
	Inc_namespace_number_filtered_msg func(namespace string)
	Observe_push_time                 func(t time.Duration)
	Inc_monitors_ticks                func(namespace string)
	Inc_monitors_sent                 func(namespace string, pulsar_event string)

	activate_observe_processing_time bool
}

func (prom_metric *Prom_metrics) registor(reg *prometheus.Registry) {
	reg.MustRegister(prom_metric.namespace_count)
	reg.MustRegister(prom_metric.pulsar_processed_msg)
	if prom_metric.activate_observe_processing_time {
		reg.MustRegister(prom_metric.pulsar_processed_msg_time)
	}

	if prom_metric.activate_observe_processing_time {
		reg.MustRegister(prom_metric.filter_time)
	}
	reg.MustRegister(Prom_metric.filtered_msg)

	// if prom_metric.activate_observe_processing_time {
	// 	reg.MustRegister(prom_metric.number_of_metrics_per_msg)
	// }

	if prom_metric.activate_observe_processing_time {
		reg.MustRegister(prom_metric.push_time)
	}
	reg.MustRegister(prom_metric.monitors_ticks_generated)
	reg.MustRegister(prom_metric.monitors_sent)
}

func create_prom_metric(activate_observe_processing_time bool) *Prom_metrics {
	prom_metric := &Prom_metrics{
		activate_observe_processing_time: activate_observe_processing_time,

		namespace_count: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "namespace_count",
				Help: "The total number of namespaces",
			},
		),
		pulsar_processed_msg: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "pulsar_processed_msg",
				Help: "The total number of processed messages from pulsar.",
			},
		),
		pulsar_processed_msg_time: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "pulsar_processed_msg_time",
				Help:       "The time to process a message from pulsar (µs)",
				Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
			},
		),
		filter_time: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "filter_time",
				Help:       "The time to apply all filters to a message (µs)",
				Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
			},
		),
		filtered_msg: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "filtered_messages",
				Help: "The number of metrics generated per namespace",
			}, []string{"namespace"},
		),
		push_time: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       "push_time",
				Help:       "The time to push a filtered message per namespace (µs)",
				Objectives: map[float64]float64{0.50: 0.1, 0.80: 0.05, 0.90: 0.01, 0.95: 0.005, 0.99: 0.005},
			},
		),
		monitors_ticks_generated: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "monitors_ticks_generated",
				Help: "The number monitors requested to run",
			}, []string{"namespace"},
		),
		monitors_sent: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "monitors_sent",
				Help: "The number of monitors run and sent to pulsar",
			}, []string{"namespace", "pulsar_event"},
		),
	}

	prom_metric.Number_of_namespaces = func(n int) {
		prom_metric.namespace_count.Add(float64(n))
	}

	prom_metric.Inc_number_processed_msg = func() {
		prom_metric.pulsar_processed_msg.Inc()
	}

	if prom_metric.activate_observe_processing_time {
		prom_metric.Observe_processing_time = func(t time.Duration) {
			go prom_metric.pulsar_processed_msg_time.Observe(float64(t / time.Microsecond))
		}
	} else {
		prom_metric.Observe_processing_time = func(t time.Duration) {}
	}

	if prom_metric.activate_observe_processing_time {
		prom_metric.Observe_filter_time = func(t time.Duration) {
			go prom_metric.filter_time.Observe(float64(t / time.Microsecond))
		}
	} else {
		prom_metric.Observe_filter_time = func(t time.Duration) {}
	}

	prom_metric.Inc_namespace_number_filtered_msg = func(namespace string) {
		prom_metric.filtered_msg.With(prometheus.Labels{"namespace": namespace}).Inc()
	}

	prom_metric.Inc_monitors_ticks = func(namespace string) {
		prom_metric.monitors_ticks_generated.With(prometheus.Labels{"namespace": namespace}).Inc()
	}

	if prom_metric.activate_observe_processing_time {
		prom_metric.Observe_push_time = func(t time.Duration) {
			go prom_metric.push_time.Observe(float64(t / time.Microsecond))
		}
	} else {
		prom_metric.Observe_push_time = func(t time.Duration) {}
	}

	prom_metric.Inc_monitors_sent = func(namespace string, pulsar_event string) {
		prom_metric.monitors_sent.With(prometheus.Labels{"namespace": namespace, "pulsar_event": pulsar_event}).Inc()
	}

	return prom_metric
}

var Prom_metric *Prom_metrics

func Setup_prometheus(prometheusport uint, activate_observe_processing_time bool) {

	reg := prometheus.NewRegistry()

	Prom_metric = create_prom_metric(activate_observe_processing_time)

	Prom_metric.registor(reg)

	if prometheusport > 0 {

		http.Handle("/metrics", promhttp.HandlerFor(
			reg,
			promhttp.HandlerOpts{
				// Pass custom registry
				Registry: reg,
			},
		))

		logrus.Infof("metrics exposed at: localhost:%d/metrics", prometheusport)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", prometheusport), nil); err != nil {
			logrus.Errorf("setup prometheus: %+v", err)
		}
	}
}
