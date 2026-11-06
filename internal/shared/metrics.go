// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package shared

import "github.com/prometheus/client_golang/prometheus"

type storeMetrics struct {
	up     prometheus.Gauge
	peers  prometheus.Gauge
	lag    prometheus.Gauge
	tick   prometheus.Histogram
	errors prometheus.Counter

	credit         *prometheus.HistogramVec
	creditErrors   prometheus.Counter
	creditReclaims prometheus.Counter
}

func newStoreMetrics() *storeMetrics {
	m := &storeMetrics{
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hivenet_sync_up",
			Help: "1 while the shared-counter store answers; 0 while each router enforces its share of every limit.",
		}),
		peers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hivenet_sync_peers",
			Help: "Other live routers in this policloud, as last seen.",
		}),
		lag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hivenet_sync_lag_seconds",
			Help: "Age of the most recent peer usage entry when this router applied it.",
		}),
		tick: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "hivenet_sync_tick_seconds",
			Help:    "Duration of one sync round with the shared-counter store.",
			Buckets: []float64{.0005, .001, .002, .005, .01, .025, .05, .1, .25, .5, 1},
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hivenet_sync_errors_total",
			Help: "Sync rounds that failed to reach the shared-counter store.",
		}),
	}
	m.credit = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "hivenet_occupancy_credit_seconds",
		Help:    "Duration of one occupancy credit call to the shared store (refill).",
		Buckets: []float64{.0001, .00025, .0005, .001, .002, .005, .01, .025, .05, .1},
	}, []string{"op"})
	m.creditErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hivenet_occupancy_credit_errors_total",
		Help: "Occupancy credit calls to the shared store that failed.",
	})
	m.creditReclaims = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hivenet_occupancy_credit_reclaims_total",
		Help: "Routers taken out of the live set early because no agent was connected to them.",
	})
	m.up.Set(0)
	return m
}

// Collectors returns the store's metrics for registration.
func (s *Store) Collectors() []prometheus.Collector {
	return []prometheus.Collector{s.m.up, s.m.peers, s.m.lag, s.m.tick, s.m.errors,
		s.m.credit, s.m.creditErrors, s.m.creditReclaims}
}
