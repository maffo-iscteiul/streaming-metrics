package flow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"example.com/streaming-metrics/src/prom_metrics"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

func Alarm_ticker(namespace *Namespace, monitor_tick_chan chan<- *string) {
	ticker := time.NewTicker(namespace.interval())
	defer ticker.Stop()

	logrus.Infof("creating monitor: %s %s", namespace.Namespace, namespace.interval().String())

	for {
		<-ticker.C

		monitor_tick_chan <- &namespace.Namespace
		prom_metrics.Prom_metric.Inc_monitors_ticks(namespace.Namespace)
	}
}

func Alarm(namespaces map[string]*Namespace, monitor_tick_chan <-chan *string, write_chan chan<- *Write_struct) {
	for monitor := range monitor_tick_chan {

		logrus.Debugf("Running monitor: %s", *monitor)

		namespace := namespaces[*monitor]

		iter := namespace.monitor.Run(namespace.gojq_namespace())
		for {
			//fmt.Printf("%#v\n", iter)
			v, ok := iter.Next()

			if !ok {
				break
			}
			if err, ok := v.(error); ok {
				logrus.Errorf("Alarm %s: %+v", namespace.Namespace, err)
				continue
			} else {
				logrus.Debugf("%+v", v)
				payload, err := json.Marshal(v)
				if err != nil {
					logrus.Errorf("Alarm marshal: %+v", err)
					continue
				}
				write_chan <- &Write_struct{
					namespace: namespace.Namespace,
					monitor:   payload,
				}
			}
		}
	}

}

type Write_struct struct {
	namespace string
	monitor   []byte
}

func Producer(write_chan <-chan *Write_struct, producer pulsar.Producer) {
	for monitor := range write_chan {
		producer.SendAsync(
			context.Background(),
			&pulsar.ProducerMessage{
				Payload: monitor.monitor,
				Key:     monitor.namespace,
			},
			send_callback(monitor.namespace),
		)
	}
}

func send_callback(namespace string) func(msgID pulsar.MessageID, pm *pulsar.ProducerMessage, err error) {
	return func(msgID pulsar.MessageID, pm *pulsar.ProducerMessage, err error) {
		if err != nil {
			prom_metrics.Prom_metric.Inc_monitors_sent(namespace, fmt.Sprintf("%v", err))
		} else {
			prom_metrics.Prom_metric.Inc_monitors_sent(namespace, "ok")
		}
	}
}
