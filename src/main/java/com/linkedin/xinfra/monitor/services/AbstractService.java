/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services;

import com.linkedin.xinfra.monitor.common.Utils;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractService implements Service {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractService.class);
  // Below fields are used for the topic description retry logic since sometimes it takes a while for the admin clint
  // to discover a topic due to the fact that Kafka's metadata is eventually consistent. The retry logic is particularly
  // helpful to avoid exceptions when a new topic gets created since it takes even longer for the admin client to discover
  // the newly created topic
  private final int _describeTopicRetries;
  private final Duration _describeTopicRetryInterval;

  AbstractService(int describeTopicRetries, Duration describeTopicRetryInterval) {
    if (describeTopicRetries < 1) {
      throw new IllegalArgumentException("Expect retry greater 0. Got: " + describeTopicRetries);
    }
    _describeTopicRetries = describeTopicRetries;
    _describeTopicRetryInterval = describeTopicRetryInterval;
  }

  TopicDescription getTopicDescription(AdminClient adminClient, String topic) {
    int attemptCount = 0;
    TopicDescription topicDescription = null;
    Exception exception = null;

    while (attemptCount < _describeTopicRetries) {
      DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topic));
      Map<String, KafkaFuture<TopicDescription>> topicResultValues = describeTopicsResult.values();
      KafkaFuture<TopicDescription> topicDescriptionKafkaFuture = topicResultValues.get(topic);
      topicDescription = null;
      exception = null;
      try {
        topicDescription = topicDescriptionKafkaFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        exception = e;
      }
      if (exception != null) {
        LOG.error("Exception occurred while getting the topicDescriptionKafkaFuture for topic: {} at attempt {}", topic,
            attemptCount, exception);
      } else if (topicDescription == null) {
        LOG.warn("Got null description for topic {} at attempt {}", topic, attemptCount);
      } else {
        return topicDescription;
      }
      attemptCount++;
      if (attemptCount < _describeTopicRetries) {
        Utils.delay(_describeTopicRetryInterval);
      }
    }

    if (exception != null) {
      throw new IllegalStateException(exception);
    } else {
      throw new IllegalStateException(String.format("Got null description for topic %s after %d retry(s)", topic, _describeTopicRetries));
    }
  }
}
