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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Metrics sub-class for Cluster Topic Manipulation Service that extends the parent class XinfraMonitorMetrics.
 */
public class ClusterTopicManipulationMetrics extends XinfraMonitorMetrics {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTopicManipulationMetrics.class);
  private final Sensor _topicCreationSensor;
  private final Sensor _topicDeletionSensor;
  private long _topicCreationStartTimeMs;
  private long _topicDeletionStartTimeMs;
  public static final String METRIC_GROUP_NAME = "cluster-topic-manipulation-service";

  /**
   *
   * @param metrics a named, numerical measurement. sensor is a handle to record numerical measurements as they occur.
   * @param tags metrics/sensor's tags
   */
  public ClusterTopicManipulationMetrics(final Metrics metrics, final Map<String, String> tags) {
    super(metrics, tags);
    _topicCreationSensor = metrics.sensor("topic-creation-metadata-propagation");
    _topicDeletionSensor = metrics.sensor("topic-deletion-metadata-propagation");
    _topicCreationSensor.add(new MetricName("topic-creation-metadata-propagation-ms-avg", METRIC_GROUP_NAME,
        "The average propagation duration in ms of propagating topic creation data and metadata to all brokers in the cluster",
        tags), new Avg());
    _topicCreationSensor.add(new MetricName("topic-creation-metadata-propagation-ms-max", METRIC_GROUP_NAME,
        "The maximum propagation time in ms of propagating topic creation data and metadata to all brokers in the cluster",
        tags), new Max());
    _topicDeletionSensor.add(new MetricName("topic-deletion-metadata-propagation-ms-avg", METRIC_GROUP_NAME,
        "The average propagation duration in milliseconds of propagating the topic deletion data and metadata "
            + "across all the brokers in the cluster.", tags), new Avg());
    _topicDeletionSensor.add(new MetricName("topic-deletion-metadata-propagation-ms-max", METRIC_GROUP_NAME,
        "The maximum propagation time in milliseconds of propagating the topic deletion data and metadata "
            + "across all the brokers in the cluster.", tags), new Max());

    LOGGER.debug("{} constructor was initialized successfully.", "ClusterTopicManipulationMetrics");
  }

  /**
   * start measuring the topic creation process and its RPC (remote programmable client)
   */
  public void startTopicCreationMeasurement() {
    this.setTopicCreationStartTimeMs(System.currentTimeMillis());
    LOGGER.debug("Started measuring.");
  }

  public void startTopicDeletionMeasurement() {
    this.setTopicDeletionStartTimeMs(System.currentTimeMillis());
    LOGGER.debug("Started measuring the cluster topic deletion process.");
  }

  /**
   *
   * @param millis time in milliseconds in long data type
   */
  void setTopicCreationStartTimeMs(long millis) {
    _topicCreationStartTimeMs = millis;
  }

  /**
   *
   * @param millis time in milli-seconds as a long data type
   */
  void setTopicDeletionStartTimeMs(long millis) {
    _topicDeletionStartTimeMs = millis;
  }

  /**
   *
   */
  public void finishTopicCreationMeasurement() {
    long completedMs = System.currentTimeMillis();
    long startMs = this.topicCreationStartTimeMs();
    this._topicCreationSensor.record(completedMs - startMs);

    LOGGER.debug("Finished measuring topic creation.");
  }

  public void finishTopicDeletionMeasurement() {
    long completeMs = System.currentTimeMillis();
    long startMs = this.topicDeletionStartTimeMs();
    this._topicDeletionSensor.record(completeMs - startMs);

    LOGGER.debug("Finished measuring topic deletion");
  }

  /**
   *
   * @return the _topicCreationStartTimeMs as a long data type
   */
  private long topicCreationStartTimeMs() {
    return _topicCreationStartTimeMs;
  }

  private long topicDeletionStartTimeMs() {
    return _topicDeletionStartTimeMs;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}


