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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@SuppressWarnings("unchecked")
@Test
public class MultiClusterTopicManagementServiceTest {
  private static final String TOPIC = "xinfra-monitor-MultiClusterTopicManagementServiceTest-topic";
  private static Set<Node> nodeSet;
  private MultiClusterTopicManagementService.TopicManagementHelper _topicManagementHelper;
  private CreateTopicsResult _createTopicsResult;
  private Map<String, KafkaFuture<Void>> _kafkaFutureMap;
  private KafkaFuture<Void> _kafkaFuture;

  @BeforeMethod
  private void start() {
    _createTopicsResult = Mockito.mock(CreateTopicsResult.class);
    _kafkaFutureMap = Mockito.mock(Map.class);
    _kafkaFuture = Mockito.mock(KafkaFuture.class);

    nodeSet = new HashSet<>();
    nodeSet.add(new Node(1, "host-1", 2132));
    nodeSet.add(new Node(2, "host-2", 2133));
    nodeSet.add(new Node(3, "host-3", 2134));

    _topicManagementHelper = Mockito.mock(MultiClusterTopicManagementService.TopicManagementHelper.class);
    _topicManagementHelper._topic = TOPIC;
    _topicManagementHelper._adminClient = Mockito.mock(AdminClient.class);
    _topicManagementHelper._topicCreationEnabled = true;
  }

  @AfterMethod
  private void finish() {
    System.out.println("Finished " + this.getClass().getCanonicalName().toLowerCase() + ".");
  }

  @Test
  protected void topicCreationTest() throws Exception {

    Mockito.doCallRealMethod().when(_topicManagementHelper).maybeCreateTopic();

    Mockito.when(_topicManagementHelper._adminClient.describeCluster())
        .thenReturn(Mockito.mock(DescribeClusterResult.class));
    Mockito.when(_topicManagementHelper._adminClient.describeCluster().nodes())
        .thenReturn(Mockito.mock(KafkaFuture.class));
    Mockito.when(_topicManagementHelper._adminClient.describeCluster().nodes().get()).thenReturn(nodeSet);

    Mockito.when(_topicManagementHelper._adminClient.createTopics(Mockito.anyCollection()))
        .thenReturn(_createTopicsResult);
    Mockito.when(_topicManagementHelper._adminClient.createTopics(Mockito.anyCollection()).values())
        .thenReturn(_kafkaFutureMap);
    Mockito.when(_topicManagementHelper._adminClient.createTopics(Mockito.anyCollection()).values().get(TOPIC))
        .thenReturn(_kafkaFuture);
    Mockito.when(_topicManagementHelper._adminClient.createTopics(Mockito.anyCollection()).values().get(TOPIC).get())
        .thenAnswer(new Answer<Object>() {
          /**
           * @param invocation the invocation on the mocked TopicManagementHelper.
           * @return NULL value
           * @throws Throwable the throwable to be thrown when Exception occurs.
           */
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            Mockito.when(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(TOPIC)))
                .thenReturn(Mockito.mock(DescribeTopicsResult.class));
            Mockito.when(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(TOPIC)).values())
                .thenReturn(Mockito.mock(Map.class));
            Mockito.when(
                _topicManagementHelper._adminClient.describeTopics(Collections.singleton(TOPIC)).values().get(TOPIC))
                .thenReturn(Mockito.mock(KafkaFuture.class));
            Mockito.when(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(TOPIC))
                .values()
                .get(TOPIC)
                .get()).thenReturn(Mockito.mock(TopicDescription.class));
            Mockito.when(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(TOPIC))
                .values()
                .get(TOPIC)
                .get()
                .name()).thenReturn(TOPIC);
            return null;
          }
        });
    _topicManagementHelper.maybeCreateTopic();

    Assert.assertNotNull(
        _topicManagementHelper._adminClient.describeTopics(Collections.singleton(TOPIC)).values().get(TOPIC).get());
    Assert.assertEquals(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(TOPIC))
        .values()
        .get(TOPIC)
        .get()
        .name(), TOPIC);
  }
}
