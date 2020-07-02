/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.common;

import com.linkedin.xinfra.monitor.consumer.NewConsumer;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerGroupCoordinatorUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(NewConsumer.class);
  private static final String CONSUMER_GROUP_PREFIX_CANDIDATE = "__shadow_consumer_group-";

  /**
   * https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L189
   * The consumer group string's hash code is used for this modulo operation.
   * @param groupId kafka consumer group ID
   * @param consumerOffsetsTopicPartitions number of partitions in the __consumer_offsets topic.
   * @return hashed integer which represents a number, the Kafka's Utils.abs() value of which is the broker
   * ID of the group coordinator, or the leader of the offsets topic partition.
   */
  public static int partitionFor(String groupId, int consumerOffsetsTopicPartitions) {

    LOGGER.debug("Hashed and modulo output: {}", groupId.hashCode());
    return Utils.abs(groupId.hashCode()) % consumerOffsetsTopicPartitions;
  }

  /**
   * Instead of making targetGroupId an instance variable and then assigning it some value which this then looks up
   * it can just be a parameter to a method
   * hash(group.id) % (number of __consumer_offsets topic partitions).
   * The partition's leader is the group coordinator
   * Choose B s.t hash(A) % (number of __consumer_offsets topic partitions) == hash(B) % (number of __consumer_offsets topic partitions)
   * @param targetGroupId the identifier of the target consumer group
   * @param adminClient an Admin Client object
   */
  public static String findCollision(String targetGroupId, AdminClient adminClient)
      throws ExecutionException, InterruptedException {
    if (targetGroupId.equals("")) {
      throw new IllegalArgumentException("The target consumer group identifier cannot be empty: " + targetGroupId);
    }

    int numOffsetsTopicPartitions = adminClient.describeTopics(Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME))
        .values()
        .get(Topic.GROUP_METADATA_TOPIC_NAME)
        .get()
        .partitions()
        .size();

    // Extract invariant from loop
    int targetConsumerOffsetsPartition = partitionFor(targetGroupId, numOffsetsTopicPartitions);

    //  This doesn't need to be an instance variable because we throw this out this value at the end of computation
    int groupSuffix = 0;

    // Extract return value so it's not computed twice, this reduces the possibility of bugs
    String newConsumerGroup;

    // Use while(true) otherwise halting condition is hard to read.
    while (true) {
      // TODO: could play fancy StringBuilder games here to make this generate less garbage
      newConsumerGroup = CONSUMER_GROUP_PREFIX_CANDIDATE + groupSuffix++;
      int newGroupNamePartition = ConsumerGroupCoordinatorUtils.partitionFor(newConsumerGroup, numOffsetsTopicPartitions);
      if (newGroupNamePartition == targetConsumerOffsetsPartition) {
        break;
      }
    }

    return newConsumerGroup;
  }
}

