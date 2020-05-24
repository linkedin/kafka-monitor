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

  @BeforeMethod
  public void beforeMethod() {
  }

  @AfterMethod
  public void afterMethod() {
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConsumerGroupCoordinatorHashing() throws ExecutionException, InterruptedException {
    Properties consumerProperties = new Properties();

    AdminClient adminClient = Mockito.mock(AdminClient.class);
    NewConsumer newConsumer = Mockito.mock(NewConsumer.class);

    Mockito.doCallRealMethod().when(newConsumer).configureGroupId(Mockito.any(), Mockito.any());
    Mockito.doCallRealMethod().when(newConsumer).consumerGroupCoordinatorHasher(Mockito.anyString(), Mockito.anyInt());

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
        .size()).thenReturn(5);

    newConsumer._targetConsumerGroupId = "target-group-id";
    newConsumer._consumerGroupPrefix = "__shadow_consumer_group-";
    newConsumer._consumerGroupSuffixCandidate = 0;
    newConsumer.configureGroupId(consumerProperties, adminClient);
    System.out.println("Consumer properties after configuration: " + consumerProperties);
    Assert.assertNotNull(consumerProperties.get("group.id"));

    int hashedResult = newConsumer.consumerGroupCoordinatorHasher(consumerProperties.get("group.id").toString(), 5);
    int hashedResult2 = newConsumer.consumerGroupCoordinatorHasher(newConsumer._targetConsumerGroupId, 5);
    System.out.println("Modulo result as an absolute value: " + Math.abs(hashedResult));
    System.out.println("Modulo result as an absolute value: " + Math.abs(hashedResult2));
    Assert.assertEquals(hashedResult, hashedResult2);
  }
}
