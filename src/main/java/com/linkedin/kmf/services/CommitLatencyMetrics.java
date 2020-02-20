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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The CommitLatencyMetrics class contains methods that measures and
 * determines the latency of Kafka consumer offset commit().
 */
public class CommitLatencyMetrics {
  private static final String METRIC_GROUP_NAME = "commit-latency-service";
  private static final Logger LOG = LoggerFactory.getLogger(CommitLatencyMetrics.class);
  final Sensor _commitOffsetLatency;
  long _committedMs;

  /**
   * Metrics for Calculating the offset commit latency of a consumer.
   * @param metrics the commit offset metrics
   * @param tags the tags associated, i.e) kmf.services:name=single-cluster-monitor
   */
  CommitLatencyMetrics(final Metrics metrics,
                       final Map<String, String> tags,
                       final int latencyPercentileMaxMs,
                       final int latencyPercentileGranularityMs) {
    LOG.info("{} was called.", this.getClass().getSimpleName());

    _commitOffsetLatency = metrics.sensor("commit-offset-latency");
    _commitOffsetLatency.add(new MetricName("commit-offset-latency-ms-avg", METRIC_GROUP_NAME, "The average latency in ms of committing offset", tags), new Avg());
    _commitOffsetLatency.add(new MetricName("commit-offset-latency-ms-max", METRIC_GROUP_NAME, "The maximum latency in ms of committing offset", tags), new Max());

    int bucketNum = latencyPercentileMaxMs / latencyPercentileGranularityMs + 2;
    int sizeInBytes = bucketNum * 4;
    _commitOffsetLatency.add(new Percentiles(sizeInBytes, latencyPercentileMaxMs, Percentiles.BucketSizing.CONSTANT,
        new Percentile(new MetricName("commit-offset-latency-ms-99th", METRIC_GROUP_NAME, "The 99th percentile latency of committing offset", tags), 99.0),
        new Percentile(new MetricName("commit-offset-latency-ms-999th", METRIC_GROUP_NAME, "The 99.9th percentile latency of committing offset", tags), 99.9),
        new Percentile(new MetricName("commit-offset-latency-ms-9999th", METRIC_GROUP_NAME, "The 99.99th percentile latency of committing offset", tags), 99.99)));

  }

  public void setCommittedMs(long time) {
    _committedMs = time;
  }

}
