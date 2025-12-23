package main

import (
	"github.com/jnovack/flag"
)

type opt struct {
	sourcepulsar                  string
	sourcetopic                   string
	sourcesubscription            string
	sourcename                    string
	sourcetrustcerts              string
	sourcecertfile                string
	sourcekeyfile                 string
	sourceallowinsecureconnection bool

	destpulsar                  string
	desttopic                   string
	destsubscription            string
	destname                    string
	desttrustcerts              string
	destcertfile                string
	destkeyfile                 string
	destallowinsecureconnection bool

	batchmaxpublishdelay uint
	batchmaxmessages     uint
	batchingmaxsize      uint

	consumerthreads uint
	monitorthreads  uint

	monitorsdir string

	pprofon       bool
	pprofdir      string
	pprofduration uint

	prometheusport                   uint
	activate_observe_processing_time bool

	loglevel string

	tickerseconds uint
}

func from_args() opt {

	var opt opt

	flag.StringVar(&opt.sourcepulsar, "source_pulsar", "pulsar://localhost:6650", "Source pulsar address")
	flag.StringVar(&opt.sourcetopic, "source_topic", "persistent://public/default/in", "Source topic names (seperated by ;)")
	flag.StringVar(&opt.sourcesubscription, "source_subscription", "streaming_monitors", "Source subscription name")
	flag.StringVar(&opt.sourcename, "source_name", "streaming_monitors_consumer", "Source consumer name")
	flag.StringVar(&opt.sourcetrustcerts, "source_trust_certs", "", "Path for source pem file, for ca.cert")
	flag.StringVar(&opt.sourcecertfile, "source_cert_file", "", "Path for source cert.pem file")
	flag.StringVar(&opt.sourcekeyfile, "source_key_file", "", "Path for source key-pk8.pem file")
	flag.BoolVar(&opt.sourceallowinsecureconnection, "source_allow_insecure_connection", false, "Source allow insecure connection")

	flag.StringVar(&opt.destpulsar, "dest_pulsar", "pulsar://localhost:6650", "Destination pulsar address")
	flag.StringVar(&opt.desttopic, "dest_topic", "persistent://public/default/out", "Destination topic name")
	flag.StringVar(&opt.destsubscription, "dest_subscription", "streaming_monitors", "Destination subscription name")
	flag.StringVar(&opt.destname, "dest_name", "streaming_monitors_consumer", "Destination producer name")
	flag.StringVar(&opt.desttrustcerts, "dest_trust_certs", "", "Path for destination pem file, for ca.cert")
	flag.StringVar(&opt.destcertfile, "dest_cert_file", "", "Path for destination cert.pem file")
	flag.StringVar(&opt.destkeyfile, "dest_key_file", "", "Path for destination key-pk8.pem file")
	flag.BoolVar(&opt.destallowinsecureconnection, "dest_allow_insecure_connection", false, "Dest allow insecure connection")

	flag.UintVar(&opt.batchmaxpublishdelay, "batch_max_publish_delay", 100, "How long to wait for batching in milliseconds")
	flag.UintVar(&opt.batchmaxmessages, "batch_max_messages", 300, "Max batch messages")
	flag.UintVar(&opt.batchingmaxsize, "batch_max_size", 131072, "Max batch size in bytes")

	flag.UintVar(&opt.consumerthreads, "consumer_threads", 6, "Number of threads to consume from pulsar")
	flag.UintVar(&opt.monitorthreads, "monitor_threads", 2, "Number of threads for monitors")

	flag.StringVar(&opt.monitorsdir, "monitors_dir", "./monitors", "Directory of all the jq monitors files")

	flag.BoolVar(&opt.pprofon, "pprof_on", false, "Profoling on?")
	flag.StringVar(&opt.pprofdir, "pprof_dir", "./pprof", "Directory for pprof file")
	flag.UintVar(&opt.pprofduration, "pprof_duration", 60*2, "Number of seconds to run pprof")

	flag.UintVar(&opt.prometheusport, "prometheus_port", 7700, "Prometheous port")
	flag.BoolVar(&opt.activate_observe_processing_time, "activatete_timing_colection", false, "Is the collection by prometheus of processing time on (may hinder perforance!)")

	flag.StringVar(&opt.loglevel, "log_level", "info", "Logging level: panic - fatal - error - warn - info - debug - trace")

	flag.UintVar(&opt.tickerseconds, "ticker_seconds", 1, "tickerseconds")

	flag.Parse()

	return opt

}
