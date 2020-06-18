/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services.metrics;

import java.util.Map;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO -- in progress!
 */
public class ClusterTopicManipulationMetrics {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTopicManipulationMetrics.class);
  public Metrics _metrics;
  private final Sensor _topicCreationSensor;
  private final Sensor _topicDeletionSensor;
  private long _topicCreationStartTimeMs;
  private long _topicDeletionStartTimeMs;
  private final Map<String, String> _tags;

  /**
   *
   * @param metrics metrics
   * @param tags tags
   */
  public ClusterTopicManipulationMetrics(final Metrics metrics, final Map<String, String> tags) {
    _metrics = metrics;
    _tags = tags;
    _topicCreationSensor = metrics.sensor("topic-creation-metadata-propagation");
    _topicDeletionSensor = metrics.sensor("topic-deletion-metadata-propagation");
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

    LOGGER.debug("Finished measuring cluster topic deletion");
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

}


