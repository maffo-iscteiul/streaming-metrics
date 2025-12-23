package main

import (
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsar_log "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/sirupsen/logrus"

	"example.com/streaming-metrics/src/flow"
	"example.com/streaming-metrics/src/prom_metrics"
)

func logging(level string) {
	logrus.SetFormatter(&logrus.JSONFormatter{
		//FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
	})
	l, err := logrus.ParseLevel(level)
	if err != nil {
		logrus.Errorf("Failed parse log level. Reason: %+v", err)
	} else {
		logrus.SetLevel(l)
	}
}

func new_client(url string, trust_cert_file string, cert_file string, key_file string, allow_insecure_connection bool) pulsar.Client {
	var client pulsar.Client
	var err error
	var auth pulsar.Authentication

	if len(cert_file) > 0 || len(key_file) > 0 {
		auth = pulsar.NewAuthenticationTLS(cert_file, key_file)
	}

	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
	})

	client, err = pulsar.NewClient(pulsar.ClientOptions{
		URL:                        url,
		TLSAllowInsecureConnection: allow_insecure_connection,
		Authentication:             auth,
		TLSTrustCertsFilePath:      trust_cert_file,
		Logger:                     pulsar_log.NewLoggerWithLogrus(log),
	})

	if err != nil {
		logrus.Errorf("Failed connect to pulsar. Reason: %+v", err)
	}
	return client
}

func main() {
	opt := from_args()
	logging(opt.loglevel)
	logrus.Infof("%+v", opt)

	go prom_metrics.Setup_prometheus(opt.prometheusport, opt.activate_observe_processing_time)

	// Clients
	source_client := new_client(opt.sourcepulsar, opt.sourcetrustcerts, opt.sourcecertfile, opt.sourcekeyfile, opt.sourceallowinsecureconnection)
	dest_client := new_client(opt.destpulsar, opt.desttrustcerts, opt.destcertfile, opt.destkeyfile, opt.destallowinsecureconnection)

	defer source_client.Close()
	defer dest_client.Close()

	consume_chan := make(chan pulsar.ConsumerMessage, 2000)
	monitor_ticker_chan := make(chan *string, 500)
	write_chan := make(chan *flow.Write_struct, 2000)
	ack_chan := make(chan pulsar.ConsumerMessage, 2000)

	consumer, err := source_client.Subscribe(pulsar.ConsumerOptions{
		Topics:                      strings.Split(opt.sourcetopic, ";"),
		SubscriptionName:            opt.sourcesubscription,
		Name:                        opt.sourcename,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		MessageChannel:              consume_chan,
		ReceiverQueueSize:           2000,
	})
	if err != nil {
		logrus.Fatalln("Failed create consumer. Reason: ", err)
	}

	producer, err := dest_client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   opt.desttopic,
		Name:                    opt.destname,
		BatchingMaxPublishDelay: time.Millisecond * time.Duration(opt.batchmaxpublishdelay),
		BatchingMaxMessages:     opt.batchmaxmessages,
		BatchingMaxSize:         opt.batchingmaxsize,
	})
	if err != nil {
		logrus.Fatalln("Failed create producer. Reason: ", err)
	}

	defer consumer.Close()
	defer producer.Close()

	configs := load_configs(opt.monitorsdir)
	namespaces := load_namespaces(opt.monitorsdir, configs)
	filters := load_filters(opt.monitorsdir, configs)

	prom_metrics.Prom_metric.Number_of_namespaces(len(namespaces))

	// Logic
	go flow.Producer(write_chan, producer)

	tick := time.NewTicker(time.Second * time.Duration(opt.tickerseconds))
	for i := 0; i < int(opt.consumerthreads); i++ {
		go flow.Consumer(consume_chan, ack_chan, namespaces, filters, tick.C)
	}

	for i := 0; i < int(opt.monitorthreads); i++ {
		go flow.Alarm(namespaces, monitor_ticker_chan, write_chan)
	}

	for _, namespace := range namespaces {
		go flow.Alarm_ticker(namespace, monitor_ticker_chan)
	}

	if opt.pprofon {
		go activate_profiling(opt.pprofdir, time.Duration(opt.pprofduration)*time.Second)
	}

	flow.Acks(consumer, ack_chan)
}
