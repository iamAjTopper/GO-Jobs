package metrics

import (
	 "github.com/prometheus/client_golang/prometheus"
)

//crezting a counter for successfull jobs
var JobProcessed = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name:"jobs_processed_total",
		Help:"Total number of processed jobs",
	},
)

//creating counter for the fail;ed jobs
var JobsFailed = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "jobs_failed_total",
		Help: "Total number of failed jobs",
	},
)

func Init() {
	prometheus.MustRegister(JobProcessed)
	prometheus.MustRegister(JobsFailed)
}