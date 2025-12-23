package flow

import (
	"encoding/json"
	"time"

	"example.com/streaming-metrics/src/prom_metrics"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

func Consumer(consume_chan <-chan pulsar.ConsumerMessage, ack_chan chan<- pulsar.ConsumerMessage, namespaces map[string]*Namespace, filters *Filter_root, tick <-chan time.Time) {
	var n_read float64 = 0

	last_instant := time.Now()
	last_publish_time := time.Unix(0, 0)
	log_tick := time.NewTicker(time.Minute)
	defer log_tick.Stop()

	for {
		select {
		case msg := <-consume_chan:
			n_read += 1
			last_publish_time = msg.PublishTime()

			consume_start := time.Now()

			metrics := filter(msg.Payload(), filters)

			filter_dur := time.Since(consume_start)

			push_start := time.Now()
			for i := 0; i < len(metrics); i++ {
				metric := &metrics[i]
				prom_metrics.Prom_metric.Inc_namespace_number_filtered_msg(metric.namespace)
				if namespace, ok := namespaces[metric.namespace]; ok {
					namespace.push(metric)
				} else {
					logrus.Errorf("No namespace named: %s", metric.namespace)
				}
			}
			push_dur := time.Since(push_start)
			prom_metrics.Prom_metric.Observe_push_time(push_dur)
			ack_chan <- msg

			proccess_dur := time.Since(consume_start)
			prom_metrics.Prom_metric.Observe_filter_time(filter_dur)
			prom_metrics.Prom_metric.Observe_processing_time(proccess_dur)

		case <-tick:
			for _, namespace := range namespaces {
				namespace.tick(last_publish_time)
			}

		case <-log_tick.C:
			since := time.Since(last_instant)
			last_instant = time.Now()
			logrus.Infof("Read rate: %.3f msg/s; (last pulsar time %v)", n_read/float64(since/time.Second), last_publish_time)
			n_read = 0
		}
	}
}

func filter(msg []byte, filters *Filter_root) []Metric {
	filtered := make([]Metric, 0)

	var msg_json any

	if err := json.Unmarshal(msg, &msg_json); err != nil {
		logrus.Errorf("filter unmarshal msg: %+v", err)
		return filtered
	}

	// filter_start := time.Now()

	iter := filters.group_filter.Run(msg_json)

	v, ok := iter.Next()
	if !ok {
		return filtered
	}
	if _, ok := v.(error); ok {
		// ignore -- msg is not important for this namespace
		logrus.Tracef("filter group_filter next err: %+v", v.(error))
		return filtered
	}

	var group_name string

	switch gn := v.(type) {
	case string:
		group_name = gn

	default:
		logrus.Errorf("filter_root did not return string: %+v", v)
		return filtered
	}

	group_filters, ok := filters.groups[group_name]
	if !ok {
		logrus.Errorf("filter_root group does not exist: %s", group_name)
		return filtered
	}

	for _, filter := range group_filters.children {
		iter := filter.Filter.Run(msg_json)

		v, ok := iter.Next()
		if !ok {
			continue
		}
		if _, ok := v.(error); ok {
			// ignore -- msg is not important for this namespace
			logrus.Tracef("filter next err: %+v", v.(error))
			continue
		} else {
			metric := metric_from_any(v)

			if metric != nil {
				filtered = append(filtered, *metric)
			}
		}
	}

	// go prom_metrics.Filter_time.Observe(float64(time.Since(filter_start) / time.Microsecond))
	// go prom_metrics.Number_of_metrics_per_msg.Observe(float64(len(filtered)))

	return filtered
}

func Acks(consumer pulsar.Consumer, ack_chan <-chan pulsar.ConsumerMessage) {
	last_instant := time.Now()
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()

	var ack float64 = 0
	for {
		select {
		case msg := <-ack_chan:

			if err := consumer.Ack(msg); err != nil {
				logrus.Warnf("consumer.Acks err: %+v", err)
			}
			ack++

			prom_metrics.Prom_metric.Inc_number_processed_msg()

		case <-tick.C:
			since := time.Since(last_instant)
			last_instant = time.Now()
			logrus.Infof("Ack rate: %.3f msg/s", ack/float64(since/time.Second))
			ack = 0
		}
	}
}
