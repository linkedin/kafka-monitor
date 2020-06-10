/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services;

import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumeMetrics {
  final Sensor _consumeError;
  final Sensor _bytesConsumed;
  final Sensor _recordsConsumed;
  final Sensor _recordsDuplicated;
  final Sensor _recordsLost;
  final Sensor _recordsDelay;
  final Sensor _recordsDelayed;
  private static final String METRIC_GROUP_NAME = "consume-service";
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeMetrics.class);

  ConsumeMetrics(final Metrics metrics,
      Map<String, String> tags,
      int latencyPercentileMaxMs,
      int latencyPercentileGranularityMs) {

    _bytesConsumed = metrics.sensor("bytes-consumed");
    _bytesConsumed.add(new MetricName("bytes-consumed-rate", METRIC_GROUP_NAME, "The average number of bytes per second that are consumed", tags), new Rate());

    _consumeError = metrics.sensor("consume-error");
    _consumeError.add(new MetricName("consume-error-rate", METRIC_GROUP_NAME, "The average number of errors per second", tags), new Rate());
    _consumeError.add(new MetricName("consume-error-total", METRIC_GROUP_NAME, "The total number of errors", tags), new Total());

    _recordsConsumed = metrics.sensor("records-consumed");
    _recordsConsumed.add(new MetricName("records-consumed-rate", METRIC_GROUP_NAME, "The average number of records per second that are consumed", tags), new Rate());
    _recordsConsumed.add(new MetricName("records-consumed-total", METRIC_GROUP_NAME, "The total number of records that are consumed", tags), new Total());

    _recordsDuplicated = metrics.sensor("records-duplicated");
    _recordsDuplicated.add(new MetricName("records-duplicated-rate", METRIC_GROUP_NAME, "The average number of records per second that are duplicated", tags), new Rate());
    _recordsDuplicated.add(new MetricName("records-duplicated-total", METRIC_GROUP_NAME, "The total number of records that are duplicated", tags), new Total());

    _recordsLost = metrics.sensor("records-lost");
    _recordsLost.add(new MetricName("records-lost-rate", METRIC_GROUP_NAME, "The average number of records per second that are lost", tags), new Rate());
    _recordsLost.add(new MetricName("records-lost-total", METRIC_GROUP_NAME, "The total number of records that are lost", tags), new Total());

    _recordsDelayed = metrics.sensor("records-delayed");
    _recordsDelayed.add(new MetricName("records-delayed-rate", METRIC_GROUP_NAME, "The average number of records per second that are either lost or arrive after maximum allowed latency under SLA", tags), new Rate());
    _recordsDelayed.add(new MetricName("records-delayed-total", METRIC_GROUP_NAME, "The total number of records that are either lost or arrive after maximum allowed latency under SLA", tags), new Total());

    _recordsDelay = metrics.sensor("records-delay");
    _recordsDelay.add(new MetricName("records-delay-ms-avg", METRIC_GROUP_NAME, "The average latency of records from producer to consumer", tags), new Avg());
    _recordsDelay.add(new MetricName("records-delay-ms-max", METRIC_GROUP_NAME, "The maximum latency of records from producer to consumer", tags), new Max());

    // There are 2 extra buckets use for values smaller than 0.0 or larger than max, respectively.
    int bucketNum = latencyPercentileMaxMs / latencyPercentileGranularityMs + 2;
    int sizeInBytes = 4 * bucketNum;
    _recordsDelay.add(new Percentiles(sizeInBytes, latencyPercentileMaxMs, Percentiles.BucketSizing.CONSTANT,
        new Percentile(new MetricName("records-delay-ms-99th", METRIC_GROUP_NAME, "The 99th percentile latency of records from producer to consumer", tags), 99.0),
        new Percentile(new MetricName("records-delay-ms-999th", METRIC_GROUP_NAME, "The 99.9th percentile latency of records from producer to consumer", tags), 99.9),
        new Percentile(new MetricName("records-delay-ms-9999th", METRIC_GROUP_NAME, "The 99.99th percentile latency of records from producer to consumer", tags), 99.99)));

    metrics.addMetric(new MetricName("consume-availability-avg", METRIC_GROUP_NAME, "The average consume availability", tags),
      (config, now) -> {
        double recordsConsumedRate = (double) metrics.metrics().get(metrics.metricName("records-consumed-rate", METRIC_GROUP_NAME, tags)).metricValue();
        double recordsLostRate = (double) metrics.metrics().get(metrics.metricName("records-lost-rate", METRIC_GROUP_NAME, tags)).metricValue();
        double recordsDelayedRate = (double) metrics.metrics().get(metrics.metricName("records-delayed-rate", METRIC_GROUP_NAME, tags)).metricValue();

        if (new Double(recordsLostRate).isNaN())
          recordsLostRate = 0;
        if (new Double(recordsDelayedRate).isNaN())
          recordsDelayedRate = 0;

        return recordsConsumedRate + recordsLostRate > 0
            ? (recordsConsumedRate - recordsDelayedRate) / (recordsConsumedRate + recordsLostRate) : 0;
      });
  }
}
