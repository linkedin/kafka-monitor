/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services.metrics;

import com.linkedin.xinfra.monitor.XinfraMonitorConstants;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Rate;


public class ProduceMetrics {

  public final Metrics _metrics;
  public final Sensor _recordsProduced;
  public final Sensor _produceError;
  public final Sensor _produceDelay;
  public final ConcurrentMap<Integer, Sensor> _recordsProducedPerPartition;
  public final ConcurrentMap<Integer, Sensor> _produceErrorPerPartition;
  public final ConcurrentMap<Integer, Boolean> _produceErrorInLastSendPerPartition;
  private final Map<String, String> _tags;

  public ProduceMetrics(final Metrics metrics, final Map<String, String> tags, int latencyPercentileGranularityMs,
      int latencyPercentileMaxMs, AtomicInteger partitionNumber, boolean treatZeroThroughputAsUnavailable) {
    _metrics = metrics;
    _tags = tags;

    _recordsProducedPerPartition = new ConcurrentHashMap<>();
    _produceErrorPerPartition = new ConcurrentHashMap<>();
    _produceErrorInLastSendPerPartition = new ConcurrentHashMap<>();

    _recordsProduced = metrics.sensor("records-produced");
    _recordsProduced.add(
        new MetricName("records-produced-rate", XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
            "The average number of records per second that are produced", tags), new Rate());
    _recordsProduced.add(
        new MetricName("records-produced-total", XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
            "The total number of records that are produced", tags), new CumulativeSum());

    _produceError = metrics.sensor("produce-error");
    _produceError.add(new MetricName("produce-error-rate", XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
        "The average number of errors per second", tags), new Rate());
    _produceError.add(new MetricName("produce-error-total", XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
        "The total number of errors", tags), new CumulativeSum());

    _produceDelay = metrics.sensor("produce-delay");
    _produceDelay.add(new MetricName("produce-delay-ms-avg", XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
        "The average delay in ms for produce request", tags), new Avg());
    _produceDelay.add(new MetricName("produce-delay-ms-max", XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
        "The maximum delay in ms for produce request", tags), new Max());

    // There are 2 extra buckets use for values smaller than 0.0 or larger than max, respectively.
    int bucketNum = latencyPercentileMaxMs / latencyPercentileGranularityMs + 2;
    int sizeInBytes = 4 * bucketNum;
    _produceDelay.add(new Percentiles(sizeInBytes, latencyPercentileMaxMs, Percentiles.BucketSizing.CONSTANT,
        new Percentile(new MetricName("produce-delay-ms-99th", XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
            "The 99th percentile delay in ms for produce request", tags), 99.0), new Percentile(
        new MetricName("produce-delay-ms-999th", XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
            "The 99.9th percentile delay in ms for produce request", tags), 99.9), new Percentile(
        new MetricName("produce-delay-ms-9999th", XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
            "The 99.99th percentile delay in ms for produce request", tags), 99.99)));

    metrics.addMetric(
        new MetricName("produce-availability-avg", XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
            "The average produce availability", tags), (config, now) -> {
        double availabilitySum = 0.0;
        int partitionNum = partitionNumber.get();
        for (int partition = 0; partition < partitionNum; partition++) {
          double recordsProduced = (double) metrics.metrics()
              .get(metrics.metricName("records-produced-rate-partition-" + partition,
                  XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE, tags))
              .metricValue();
          double produceError = (double) metrics.metrics()
              .get(metrics.metricName("produce-error-rate-partition-" + partition,
                  XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE, tags))
              .metricValue();
          // If there is no error, error rate sensor may expire and the value may be NaN. Treat NaN as 0 for error rate.
          if (Double.isNaN(produceError) || Double.isInfinite(produceError)) {
            produceError = 0;
          }
          // If there is either succeeded or failed produce to a partition, consider its availability as 0.
          if (recordsProduced + produceError > 0) {
            availabilitySum += recordsProduced / (recordsProduced + produceError);
          } else if (!treatZeroThroughputAsUnavailable) {
            // If user configures treatZeroThroughputAsUnavailable to be false, a partition's availability
            // is 1.0 as long as there is no exception thrown from producer.
            // This allows kafka admin to exactly monitor the availability experienced by Kafka users which
            // will block and retry for a certain amount of time based on its configuration (e.g. retries, retry.backoff.ms).
            // Note that if it takes a long time for messages to be retries and sent, the latency in the ConsumeService
            // will increase and it will reduce ConsumeAvailability if the latency exceeds consume.latency.sla.ms
            // If timeout is set to more than 60 seconds (the current samples window duration),
            // the error sample might be expired before the next error can be produced.
            // In order to detect offline partition with high producer timeout config, the error status during last
            // send is also checked before declaring 1.0 availability for the partition.
            Boolean lastSendError = _produceErrorInLastSendPerPartition.get(partition);
            if (lastSendError == null || !lastSendError) {
              availabilitySum += 1.0;
            }
          }
        }

        // Assign equal weight to per-partition availability when calculating overall availability
        return availabilitySum / partitionNum;
      }
    );
  }

  public void addPartitionSensors(int partition) {
    Sensor recordsProducedSensor = _metrics.sensor("records-produced-partition-" + partition);
    recordsProducedSensor.add(new MetricName("records-produced-rate-partition-" + partition,
        XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
        "The average number of records per second that are produced to this partition", _tags), new Rate());
    _recordsProducedPerPartition.put(partition, recordsProducedSensor);

    Sensor errorsSensor = _metrics.sensor("produce-error-partition-" + partition);
    errorsSensor.add(new MetricName("produce-error-rate-partition-" + partition,
        XinfraMonitorConstants.METRIC_GROUP_NAME_PRODUCE_SERVICE,
        "The average number of errors per second when producing to this partition", _tags), new Rate());
    _produceErrorPerPartition.put(partition, errorsSensor);
  }
}

