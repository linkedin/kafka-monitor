/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.consumer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
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
    NewConsumer newConsumer = Mockito.mock(NewConsumer.class);

    Mockito.doCallRealMethod().when(newConsumer).configureGroupId(Mockito.any(), Mockito.any());
    Mockito.doCallRealMethod().when(newConsumer).partitionsFor(Mockito.anyString(), Mockito.anyInt());

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

    newConsumer._targetConsumerGroupId = "target-group-id";
    newConsumer._consumerGroupPrefix = "__shadow_consumer_group-";
    newConsumer._consumerGroupSuffixCandidate = 0;
    newConsumer.configureGroupId(consumerProperties, adminClient);
    System.out.println("Consumer properties after configuration: " + consumerProperties);
    Assert.assertNotNull(consumerProperties.get("group.id"));

    int hashedResult = newConsumer.partitionsFor(consumerProperties.get("group.id").toString(),
        NUM_OFFSETS_TOPIC_PARTITIONS);
    int hashedResult2 = newConsumer.partitionsFor(newConsumer._targetConsumerGroupId,
        NUM_OFFSETS_TOPIC_PARTITIONS);
    System.out.println("Modulo result as an absolute value: " + Math.abs(hashedResult));
    System.out.println("Modulo result as an absolute value: " + Math.abs(hashedResult2));
    Assert.assertEquals(hashedResult, hashedResult2);
  }
}
