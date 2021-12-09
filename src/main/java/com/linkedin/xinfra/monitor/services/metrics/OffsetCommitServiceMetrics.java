/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services.metrics;

import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OffsetCommitServiceMetrics extends XinfraMonitorMetrics {

  private final Sensor _offsetCommittedSensor;
  private final Sensor _offsetCommitFailSensor;
  private static final Logger LOGGER = LoggerFactory.getLogger(OffsetCommitServiceMetrics.class);
  private static final String METRIC_GROUP_NAME = "offset-commit-service";
  private static final String SUCCESS_SENSOR_NAME = "offset-commit-service-success";
  private static final String SUCCESS_RATE_METRIC = "offset-commit-service-success-rate";
  private static final String SUCCESS_METRIC_TOTAL = "offset-commit-service-success-total";
  private static final String FAILURE_SENSOR_NAME = "offset-commit-service-failure";
  private static final String FAILURE_RATE_METRIC = "offset-commit-service-failure-rate";
  private static final String FAILURE_METRIC_TOTAL = "offset-commit-service-failure-total";

  /**
   *
   * @param metrics a named, numerical measurement.
   *                Sensor is a handle to record numerical measurements as they occur.
   * @param tags metrics/sensor's tags
   */
  public OffsetCommitServiceMetrics(final Metrics metrics, final Map<String, String> tags) {
    super(metrics, tags);
    _offsetCommittedSensor = metrics.sensor(SUCCESS_SENSOR_NAME);
    _offsetCommittedSensor.add(new MetricName(SUCCESS_RATE_METRIC, METRIC_GROUP_NAME,
        "The success rate of group coordinator accepting consumer offset commit requests.", tags), new Avg());
    _offsetCommittedSensor.add(new MetricName(SUCCESS_METRIC_TOTAL, METRIC_GROUP_NAME,
            "The total count of group coordinator successfully accepting consumer offset commit requests.", tags),
        new CumulativeSum());

    _offsetCommitFailSensor = metrics.sensor(FAILURE_SENSOR_NAME);
    /* NaN will persist as long as no record is submitted to the failure sensor.
       we'll continue with NaN for now since we'd rather that the Sensor itself is a true and unaltered record of what values it recorded. */
    _offsetCommitFailSensor.add(new MetricName(FAILURE_RATE_METRIC, METRIC_GROUP_NAME,
        "The failure rate of group coordinator accepting consumer offset commit requests.", tags), new Avg());
    _offsetCommitFailSensor.add(new MetricName(FAILURE_METRIC_TOTAL, METRIC_GROUP_NAME,
            "The total count of group coordinator unsuccessfully receiving consumer offset commit requests.", tags),
        new CumulativeSum());

    Measurable measurable = new Measurable() {
      @Override
      public double measure(MetricConfig config, long now) {
        double offsetCommitSuccessRate = (double) metrics.metrics()
            .get(metrics.metricName(SUCCESS_RATE_METRIC, METRIC_GROUP_NAME, tags))
            .metricValue();
        double offsetCommitFailureRate = (double) metrics.metrics()
            .get(metrics.metricName(FAILURE_RATE_METRIC, METRIC_GROUP_NAME, tags))
            .metricValue();

        if (new Double(offsetCommitSuccessRate).isNaN()) {
          offsetCommitSuccessRate = 0;
        }

        if (new Double(offsetCommitFailureRate).isNaN()) {
          offsetCommitFailureRate = 0;
        }

        return offsetCommitSuccessRate + offsetCommitFailureRate > 0 ? offsetCommitSuccessRate / (
            offsetCommitSuccessRate + offsetCommitFailureRate) : 0;
      }
    };

    metrics.addMetric(new MetricName("offset-commit-availability-avg", METRIC_GROUP_NAME,
        "The average offset commit availability with respect to the group coordinator.", tags), measurable);
  }

  /**
   * start measuring and its RPC (remote programmable client)
   */
  public void recordSuccessful() {
    _offsetCommittedSensor.record();
    LOGGER.debug("recorded successful.");
  }

  public void recordFailed() {
    _offsetCommitFailSensor.record();
    LOGGER.error("The offset commit failed due to the response future failing and the future NOT being retriable.");
  }

  public void recordUnavailable() {
    _offsetCommitFailSensor.record();
    LOGGER.error("The offset commit failed due to coordinator being unavailable.");
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}


