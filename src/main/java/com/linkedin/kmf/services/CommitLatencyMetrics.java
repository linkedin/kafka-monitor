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
  private final Sensor _commitOffsetLatency;
  private long _commitStartTimeMs;
  private volatile boolean _inProgressCommit;

  /**
   * Metrics for Calculating the offset commit latency of a consumer.
   * @param metrics the commit offset metrics
   * @param tags the tags associated, i.e) kmf.services:name=single-cluster-monitor
   */
  CommitLatencyMetrics(Metrics metrics, Map<String, String> tags, int latencyPercentileMaxMs, int latencyPercentileGranularityMs) {
    _inProgressCommit = false;
    _commitOffsetLatency = metrics.sensor("commit-offset-latency");
    _commitOffsetLatency.add(new MetricName("commit-offset-latency-ms-avg", METRIC_GROUP_NAME, "The average latency in ms of committing offset", tags), new Avg());
    _commitOffsetLatency.add(new MetricName("commit-offset-latency-ms-max", METRIC_GROUP_NAME, "The maximum latency in ms of committing offset", tags), new Max());

    if (latencyPercentileGranularityMs == 0) {
      throw new IllegalArgumentException("The latency percentile granularity was incorrectly passed a zero value.");
    }

    // 2 extra buckets exist which are respectively designated for values which are less than 0.0 or larger than max.
    int bucketNum = latencyPercentileMaxMs / latencyPercentileGranularityMs + 2;
    int sizeInBytes = bucketNum * 4;
    _commitOffsetLatency.add(new Percentiles(sizeInBytes, latencyPercentileMaxMs, Percentiles.BucketSizing.CONSTANT,
        new Percentile(new MetricName("commit-offset-latency-ms-99th", METRIC_GROUP_NAME, "The 99th percentile latency of committing offset", tags), 99.0),
        new Percentile(new MetricName("commit-offset-latency-ms-999th", METRIC_GROUP_NAME, "The 99.9th percentile latency of committing offset", tags), 99.9),
        new Percentile(new MetricName("commit-offset-latency-ms-9999th", METRIC_GROUP_NAME, "The 99.99th percentile latency of committing offset", tags), 99.99)));
    LOG.info("{} was constructed successfully.", this.getClass().getSimpleName());
  }

  /**
   * start the recording of consumer offset commit
   * @throws Exception if the offset commit is already in progress.
   */
  public void recordCommitStart() throws Exception {
    if (!_inProgressCommit) {
      this.setCommitStartTimeMs(System.currentTimeMillis());
      _inProgressCommit = true;
    } else {
      // inProgressCommit is already set to TRUE;
      throw new Exception("Offset commit is already in progress.");
    }
  }

  /**
   * finish the recording of consumer offset commit
   */
  public void recordCommitComplete() {
    if (_inProgressCommit) {
      long commitCompletedMs = System.currentTimeMillis();
      long commitStartMs = this.commitStartTimeMs();
      this._commitOffsetLatency.record(commitCompletedMs - commitStartMs);
      _inProgressCommit = false;
    } else {
      // inProgressCommit is already set to FALSE;
      LOG.error("Offset commit is not in progress. CommitLatencyMetrics shouldn't completing a record commit here.");
    }
  }

  /**
   * set in milliseconds the start time of consumer offset commit
   * @param time commit start time in ms
   */
  public void setCommitStartTimeMs(long time) {
    _commitStartTimeMs = time;
  }

  /**
   * retrieve the start time of consumer offset commit
   * @return _commitStartTimeMs
   */
  public long commitStartTimeMs() {
    return _commitStartTimeMs;
  }
}
