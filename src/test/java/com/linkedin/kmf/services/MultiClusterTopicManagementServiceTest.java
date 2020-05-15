/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services;

import com.linkedin.kmf.consumer.KMBaseConsumer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class MultiClusterTopicManagementServiceTest {

  @BeforeMethod
  private void start() {

    ConsumerFactory consumerFactory = Mockito.mock(ConsumerFactory.class);
    AdminClient adminClient = Mockito.mock(AdminClient.class);
    KMBaseConsumer kmBaseConsumer = Mockito.mock(KMBaseConsumer.class);

    Mockito.when(consumerFactory.adminClient()).thenReturn(adminClient);
    Mockito.when(consumerFactory.latencySlaMs()).thenReturn(20000);
    Mockito.when(consumerFactory.baseConsumer()).thenReturn(kmBaseConsumer);
    Mockito.when(consumerFactory.topic()).thenReturn("fdasl");

    /* LATENCY_PERCENTILE_MAX_MS_CONFIG, */
    Mockito.when(consumerFactory.latencyPercentileMaxMs()).thenReturn(5000);
  }

  @Test
  private void topicPartitionsWithRetryTest() throws Exception {
    MultiClusterTopicManagementService multiClusterTopicManagementService =
        Mockito.mock(MultiClusterTopicManagementService.class);
    AdminClient adminClient = Mockito.mock(AdminClient.class);

    //   new MultiClusterTopicManagementService(Mockito.mock(Map.class), "multi-cluster-topic-management-service-test");

    Map properties = new HashMap<>();

    MultiClusterTopicManagementService.TopicManagementHelper topicManagementHelper =
        Mockito.mock(MultiClusterTopicManagementService.TopicManagementHelper.class);
//        new MultiClusterTopicManagementService.TopicManagementHelper(properties);
    topicManagementHelper._adminClient = adminClient;

    Assert.assertEquals(topicManagementHelper.numPartitions(), 0);
    topicManagementHelper.maybeAddPartitions(5);
    Assert.assertEquals(topicManagementHelper.numPartitions(), 5);
  }

  @AfterMethod
  private void finish() {

  }
}
