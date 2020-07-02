/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.consumer;

import com.linkedin.xinfra.monitor.common.ConsumerGroupCoordinatorUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.internals.Topic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class NewConsumerTest {
  private static final int NUM_OFFSETS_TOPIC_PARTITIONS = 5;
  private static final String TARGET_CONSUMER_GROUP_ID = "target-group-id";

  @BeforeMethod
  public void beforeMethod() {
    System.out.println("Running beforeMethod of " + this.getClass());
  }

  @AfterMethod
  public void afterMethod() {
    System.out.println("Finished running testConsumerGroupCoordinatorHashing() of " + this.getClass());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConsumerGroupCoordinatorHashing() throws ExecutionException, InterruptedException {
    Properties consumerProperties = new Properties();

    AdminClient adminClient = Mockito.mock(AdminClient.class);

    /*
     * Mock the behavior of AdminClient only.
     */
    Mockito.when(adminClient.describeTopics(Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME)))
        .thenReturn(Mockito.mock(DescribeTopicsResult.class));
    Mockito.when(adminClient.describeTopics(Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME)).values())
        .thenReturn(Mockito.mock(Map.class));
    Mockito.when(adminClient.describeTopics(Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME))
        .values()
        .get(Topic.GROUP_METADATA_TOPIC_NAME)).thenReturn(Mockito.mock(KafkaFutureImpl.class));

    Mockito.when(adminClient.describeTopics(Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME))
        .values()
        .get(Topic.GROUP_METADATA_TOPIC_NAME)
        .get()).thenReturn(Mockito.mock(TopicDescription.class));

    Mockito.when(adminClient.describeTopics(Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME))
        .values()
        .get(Topic.GROUP_METADATA_TOPIC_NAME)
        .get()
        .partitions()).thenReturn(Mockito.mock(List.class));

    Mockito.when(adminClient.describeTopics(Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME))
        .values()
        .get(Topic.GROUP_METADATA_TOPIC_NAME)
        .get()
        .partitions()
        .size()).thenReturn(NUM_OFFSETS_TOPIC_PARTITIONS);

    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
        NewConsumer.configureGroupId(TARGET_CONSUMER_GROUP_ID, adminClient));
    System.out.println("Consumer properties after configuration: " + consumerProperties);
    Assert.assertNotNull(consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG));

    // Testing I: run partitionsFor() on the result to make sure they are the same
    int hashedResult =
        ConsumerGroupCoordinatorUtils.partitionFor(consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG).toString(),
            NUM_OFFSETS_TOPIC_PARTITIONS);
    int hashedResult2 =
        ConsumerGroupCoordinatorUtils.partitionFor(TARGET_CONSUMER_GROUP_ID, NUM_OFFSETS_TOPIC_PARTITIONS);

    Assert.assertEquals(hashedResult, hashedResult2);
    System.out.println("Modulo result as an absolute value: " + hashedResult);
    System.out.println("Modulo result as an absolute value: " + hashedResult2);

    // Testing II: Also test that the groupIds are different.
    Assert.assertNotEquals(TARGET_CONSUMER_GROUP_ID, consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG));

  }
}
