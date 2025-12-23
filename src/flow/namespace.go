package flow

import (
	"fmt"
	"time"

	"github.com/itchyny/gojq"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"

	"example.com/streaming-metrics/src/store"
	"example.com/streaming-metrics/src/store/memory_store"
)

type Metric struct {
	namespace string
	id        string
	time      string
	metric    any
}

type Namespace struct {
	Group       string `json:"group" yaml:"group"`
	Namespace   string `json:"namespace" yaml:"namespace"`
	Granularity int64  `json:"granularity" yaml:"granularity"`
	Cardinality int64  `json:"cardinality" yaml:"cardinality"`
	Snapshot    int64  `json:"snapshot" yaml:"snapshot"`
	Current     bool   `json:"current" yaml:"current"`
	Store_type  string `json:"store_type" yaml:"store_type"`

	store store.Store

	lambda  *gojq.Code
	monitor *gojq.Code
}

/*
 * Namespace
 */

func New_namesapce(buf []byte) *Namespace {
	var namespace Namespace

	if err := yaml.Unmarshal(buf, &namespace); err != nil {
		logrus.Errorf("New_namespace: %+v", err)
		return nil
	}

	if !namespace.valid_config() {
		logrus.Errorf("New_namespace: not a valid config")
		return nil
	}

	if err := namespace.create_store(); err != nil {
		logrus.Errorf("New_namespace: %+v", err)
		return nil
	}

	return &namespace
}

func (namespace *Namespace) create_store() error {
	switch namespace.Store_type {
	case "memory_store":
		namespace.store = memory_store.New_memory_store(namespace.Namespace, namespace.Granularity, namespace.Cardinality, namespace.Snapshot, namespace.Current)
	case "cached_pebble_store":
		namespace.store = memory_store.New_cached_persistent_store(namespace.Namespace, namespace.Granularity, namespace.Cardinality, namespace.Snapshot, namespace.Current)
	default:
		return fmt.Errorf("namespace.create_store %s: %s is not a valid store_type", namespace.Namespace, namespace.Store_type)
	}
	if namespace.store == nil {
		return fmt.Errorf("namespace.create_store %s: unable to create %s", namespace.Namespace, namespace.Store_type)
	}

	return nil
}

func (namespace *Namespace) Set_lambda(lambda *gojq.Code) {
	namespace.lambda = lambda
}

func (namespace *Namespace) Set_monitor(monitor *gojq.Code) {
	namespace.monitor = monitor
}

func (namespace *Namespace) push(metric *Metric) {
	if metric.namespace != namespace.Namespace {
		return
	}
	ti, err := time.Parse(time.RFC3339, metric.time)
	if err != nil {
		logrus.Warnf("namespace.push %s: %+v", namespace.Namespace, err)
		return
	}
	namespace.store.Push(metric.id, ti.Unix(), metric.metric, namespace.lambda)
}

func (namespace *Namespace) tick(t time.Time) {
	namespace.store.Tick(t.Unix())
}

func (namespace *Namespace) interval() time.Duration {
	return time.Duration(namespace.Granularity*namespace.Snapshot) * time.Second
}

func (namespace *Namespace) gojq_namespace() map[string]any {
	store_rep, current_time := namespace.store.Get_representation()
	return map[string]any{
		"namespace":   namespace.Namespace,
		"granularity": namespace.Granularity,
		"cardinality": namespace.Cardinality,
		"snapshot":    namespace.Snapshot,
		"current":     namespace.Current,
		"windows":     store_rep,
		"time":        current_time,
	}
}

func (namespace *Namespace) valid_config() bool {
	return len(namespace.Namespace) > 0 && namespace.Granularity > 0 && namespace.Cardinality > 0 && namespace.Snapshot > 0
}

func metric_from_any(in any) *Metric {
	switch v := in.(type) {
	case map[string]any:
		namespace, ok_namespace := v["namespace"].(string)
		id, ok_id := v["id"].(string)
		time, ok_time := v["time"].(string)
		metric, ok_metric := v["metric"]

		if !ok_namespace || !ok_id || !ok_time || !ok_metric {
			logrus.Errorf("metric_from_any missing field from in map filter - status: namespace(%t) id(%t) time(%t) metric(%t)", ok_namespace, ok_id, ok_time, ok_metric)
			return nil
		}

		return &Metric{
			namespace: namespace,
			id:        id,
			time:      time,
			metric:    metric,
		}
	default:
		logrus.Errorf("metric_from_any filter did not return a map: %+v", in)
		return nil
	}

}
