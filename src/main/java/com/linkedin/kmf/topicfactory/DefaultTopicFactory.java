/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.topicfactory;

import com.linkedin.kmf.common.Utils;

import java.util.Map;
import java.util.Properties;


public class DefaultTopicFactory implements TopicFactory {

  /** This constructor is required by TopicFactory but does nothing. */
  public DefaultTopicFactory(Map<String, ?> config) {
  }

  @Override
  public int createTopicIfNotExist(String zkUrl, String topic, int replicationFactor, double partitionToBrokerRatio, Properties topicConfig) {
    return Utils.createTopicIfNotExists(zkUrl, topic, replicationFactor, partitionToBrokerRatio, 1, topicConfig);
  }
}
