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
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class CommitAvailabilityMetrics {

  private static final String METRIC_GROUP_NAME = "commit-availability-service";
  private static final Logger LOG = LoggerFactory.getLogger(CommitAvailabilityMetrics.class);
  public final Sensor _offsetsCommitted;
  public final Sensor _failedCommitOffsets;

  /**
   * Metrics for Calculating the offset commit availability of a consumer.
   * @param metrics the commit offset metrics
   * @param tags the tags associated, i.e) kmf.services:name=single-cluster-monitor
   */
  CommitAvailabilityMetrics(final Metrics metrics, final Map<String, String> tags) {
    LOG.info("{} called.", this.getClass().getSimpleName());
    _offsetsCommitted = metrics.sensor("offsets-committed");
    _offsetsCommitted.add(new MetricName("offsets-committed-total", METRIC_GROUP_NAME,
        "The total number of offsets per second that are committed.", tags), new Total());

    _failedCommitOffsets = metrics.sensor("failed-commit-offsets");
    _failedCommitOffsets.add(new MetricName("failed-commit-offsets-avg", METRIC_GROUP_NAME,
        "The average number of offsets per second that have failed.", tags), new Rate());
    _failedCommitOffsets.add(new MetricName("failed-commit-offsets-total", METRIC_GROUP_NAME,
        "The total number of offsets per second that have failed.", tags), new Total());

    metrics.addMetric(new MetricName("offsets-committed-avg", METRIC_GROUP_NAME, "The average offset commits availability.", tags),
      (MetricConfig config, long now) -> {
        Object offsetCommitTotal = metrics.metrics().get(metrics.metricName("offsets-committed-total", METRIC_GROUP_NAME, tags)).metricValue();
        Object offsetCommitFailTotal = metrics.metrics().get(metrics.metricName("failed-commit-offsets-total", METRIC_GROUP_NAME, tags)).metricValue();
        if (offsetCommitTotal != null && offsetCommitFailTotal != null) {
          double offsetsCommittedCount = (double) offsetCommitTotal;
          double offsetsCommittedErrorCount = (double) offsetCommitFailTotal;
          return offsetsCommittedCount / (offsetsCommittedCount + offsetsCommittedErrorCount);
        } else {
          return 0;
        }
      });
  }
}
