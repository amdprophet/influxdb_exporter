// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	clientModel "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/promlog"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/influxdata/influxdb/models"
)

type influxDBSample struct {
	ID        string
	Name      string
	Labels    map[string]string
	Value     float64
	Timestamp time.Time
}

func parsePointsToSample(points []models.Point) {
	for _, s := range points {
		fields, err := s.Fields()
		if err != nil {
			level.Error(logger).Log("msg", "error getting fields from point", "err", err)
			continue
		}

		for field, v := range fields {
			var value float64
			switch v := v.(type) {
			case float64:
				value = v
			case int64:
				value = float64(v)
			case bool:
				if v {
					value = 1
				} else {
					value = 0
				}
			default:
				continue
			}

			var name string
			if field == "value" {
				name = string(s.Name())
			} else {
				name = string(s.Name()) + "_" + field
			}

			ReplaceInvalidChars(&name)
			sample := &influxDBSample{
				Name:      name,
				Timestamp: s.Time(),
				Value:     value,
				Labels:    map[string]string{},
			}
			for _, v := range s.Tags() {
				key := string(v.Key)
				if key == "__name__" {
					continue
				}
				ReplaceInvalidChars(&key)
				sample.Labels[key] = string(v.Value)
			}

			// Calculate a consistent unique ID for the sample.
			labelnames := make([]string, 0, len(sample.Labels))
			for k := range sample.Labels {
				labelnames = append(labelnames, k)
			}
			sort.Strings(labelnames)
			parts := make([]string, 0, len(sample.Labels)*2+1)
			parts = append(parts, name)
			for _, l := range labelnames {
				parts = append(parts, l, sample.Labels[l])
			}
			sample.ID = strings.Join(parts, ".")

			help := "InfluxDB Metric"
			mType := clientModel.MetricType_UNTYPED
			mf := clientModel.MetricFamily{
				Name:   &sample.Name,            // *string
				Help:   &help,                   // *string
				Type:   &mType,                  // *MetricType
				Metric: []*clientModel.Metric{}, // []*Metric
			}

			dtoMetric := clientModel.Metric{}
			metric := prometheus.MustNewConstMetric(
				prometheus.NewDesc(sample.Name, "InfluxDB Metric", []string{}, sample.Labels),
				prometheus.UntypedValue,
				sample.Value,
			)
			metric = prometheus.NewMetricWithTimestamp(sample.Timestamp, metric)
			if err := metric.Write(&dtoMetric); err != nil {
				handleErr(err)
			}
			mf.Metric = append(mf.Metric, &dtoMetric)

			w := io.Writer(os.Stdout)
			contentType := expfmt.FmtOpenMetrics
			enc := expfmt.NewEncoder(w, contentType)
			enc.Encode(&mf)
		}
	}
}

// analog of invalidChars = regexp.MustCompile("[^a-zA-Z0-9_]")
func ReplaceInvalidChars(in *string) {

	for charIndex, char := range *in {
		charInt := int(char)
		if !((charInt >= 97 && charInt <= 122) || // a-z
			(charInt >= 65 && charInt <= 90) || // A-Z
			(charInt >= 48 && charInt <= 57) || // 0-9
			charInt == 95) { // _

			*in = (*in)[:charIndex] + "_" + (*in)[charIndex+1:]
		}
	}
	// prefix with _ if first char is 0-9
	if int((*in)[0]) >= 48 && int((*in)[0]) <= 57 {
		*in = "_" + *in
	}
}

var logger log.Logger

func handleErr(err error) {
	fmt.Fprintf(os.Stderr, err.Error())
	os.Exit(1)
}

func init() {
	promlogConfig := &promlog.Config{}
	logger = promlog.New(promlogConfig)
}

func main() {
	file, err := os.Open(os.Args[1])
	if err != nil {
		handleErr(err)
	}

	buf, err := ioutil.ReadAll(file)
	if err != nil {
		handleErr(err)
	}

	precision := "ns"
	points, err := models.ParsePointsWithPrecision(buf, time.Now().UTC(), precision)
	if err != nil {
		handleErr(err)
	}

	parsePointsToSample(points)
}
