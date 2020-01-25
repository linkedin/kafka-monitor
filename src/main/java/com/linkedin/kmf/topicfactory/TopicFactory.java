/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.topicfactory;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;


/**
 * Constructs the monitor topic if it does not exist, and provide blacklisted brokers info for topic management service
 *
 * Implementations of this class should have a public constructor with the following signature:
 *   Constructor(Map&lt;String, ?&gt; config) where config are additional configuration parameters passed in from the Kafka
 *   Monitor configuration.
 */
public interface TopicFactory {

  /**
   * Creates the specified topic if it does not exist.
   * @param topic topic name
   * @param replicationFactor the replication factor for the topic
   * @param partitionToBrokerRatio This is multiplied by the number brokers to compute the number of partitions in the topic.
   * @param topicProperties topic specific properties, for example where you would set min.insync.replicas.  Unfortunately
   *                        the underlying createTopic() uses Properties and not Map&lt;String, ?&gt;.
   * @return The number of partitions for the specified topic.
   */

  int createTopicIfNotExist(String topic, short replicationFactor, double partitionToBrokerRatio, Properties topicProperties, AdminClient adminClient)
      throws ExecutionException, InterruptedException;

  /**
   * @param zkUrl zookeeper connection url
   * @return A set of brokers that don't take new partitions or reassigned partitions for topics.
   */
  Set<Integer> getBlackListedBrokers(String zkUrl);

}
