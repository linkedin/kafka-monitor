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

import com.linkedin.xinfra.monitor.topicfactory.TopicFactory;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kafka.admin.BrokerMetadata;
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
import scala.Option;


/**
 * Testing methods for the Xinfra Monitor class of MultiClusterTopicManagementService.
 */
@SuppressWarnings("unchecked")
@Test
public class MultiClusterTopicManagementServiceTest {

  private static final String SERVICE_TEST_TOPIC = "xinfra-monitor-Multi-Cluster-Topic-Management-Service-Test-topic";
  private static Set<Node> nodeSet;
  private MultiClusterTopicManagementService.TopicManagementHelper _topicManagementHelper;
  private CreateTopicsResult _createTopicsResult;
  private Map<String, KafkaFuture<Void>> _kafkaFutureMap;
  private KafkaFuture<Void> _kafkaFuture;

  @BeforeMethod
  private void startTest() {
    _createTopicsResult = Mockito.mock(CreateTopicsResult.class);
    _kafkaFutureMap = Mockito.mock(Map.class);
    _kafkaFuture = Mockito.mock(KafkaFuture.class);

    nodeSet = new LinkedHashSet<>();
    nodeSet.add(new Node(1, "host-1", 2132));
    nodeSet.add(new Node(2, "host-2", 2133));
    nodeSet.add(new Node(3, "host-3", 2134));
    nodeSet.add(new Node(4, "host-4", 2135));
    nodeSet.add(new Node(5, "host-5", 2136));
    nodeSet.add(new Node(6, "host-5", 2137));
    nodeSet.add(new Node(7, "host-5", 2138));
    nodeSet.add(new Node(8, "host-5", 2139));
    nodeSet.add(new Node(9, "host-5", 2140));
    nodeSet.add(new Node(10, "host-5", 2141));

    _topicManagementHelper = Mockito.mock(MultiClusterTopicManagementService.TopicManagementHelper.class);
    _topicManagementHelper._topic = SERVICE_TEST_TOPIC;
    _topicManagementHelper._adminClient = Mockito.mock(AdminClient.class);
    _topicManagementHelper._topicFactory = Mockito.mock(TopicFactory.class);
    _topicManagementHelper._topicCreationEnabled = true;
    _topicManagementHelper._topicAddPartitionEnabled = true;
    _topicManagementHelper._topicReassignPartitionAndElectLeaderEnabled = true;
  }

  @AfterMethod
  private void finishTest() {
    System.out.println("Finished " + this.getClass().getCanonicalName().toLowerCase() + ".");
  }

  @Test(invocationCount = 2)
  protected void maybeAddPartitionsTest() {
    Set<BrokerMetadata> brokerMetadataSet = new LinkedHashSet<>();
    for (Node broker : nodeSet) {
      brokerMetadataSet.add(new BrokerMetadata(broker.id(), Option.apply(broker.rack())));
    }

    int minPartitionNum = 14;
    int partitionNum = 5;
    int rf = 4;

    List<List<Integer>> newPartitionAssignments =
        MultiClusterTopicManagementService.TopicManagementHelper.newPartitionAssignments(minPartitionNum, partitionNum, brokerMetadataSet, rf);
    Assert.assertNotNull(newPartitionAssignments);

    System.out.println(newPartitionAssignments);
    Assert.assertEquals(newPartitionAssignments.size(), minPartitionNum - partitionNum);
    Assert.assertEquals(newPartitionAssignments.get(0).size(), rf);
  }

  @Test
  protected void MultiClusterTopicManagementServiceTopicCreationTest() throws Exception {

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
    Mockito.when(
        _topicManagementHelper._adminClient.createTopics(Mockito.anyCollection()).values().get(SERVICE_TEST_TOPIC))
        .thenReturn(_kafkaFuture);

    Answer<Object> createKafkaTopicFutureAnswer = new Answer<Object>() {
      /**
       * @param invocation the invocation on the mocked TopicManagementHelper.
       * @return NULL value.
       * @throws Throwable the throwable to be thrown when Exception occurs.
       */
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {

        Mockito.when(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(SERVICE_TEST_TOPIC)))
            .thenReturn(Mockito.mock(DescribeTopicsResult.class));
        Mockito.when(
            _topicManagementHelper._adminClient.describeTopics(Collections.singleton(SERVICE_TEST_TOPIC)).values())
            .thenReturn(Mockito.mock(Map.class));
        Mockito.when(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(SERVICE_TEST_TOPIC))
            .values()
            .get(SERVICE_TEST_TOPIC)).thenReturn(Mockito.mock(KafkaFuture.class));
        Mockito.when(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(SERVICE_TEST_TOPIC))
            .values()
            .get(SERVICE_TEST_TOPIC)
            .get()).thenReturn(Mockito.mock(TopicDescription.class));
        Mockito.when(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(SERVICE_TEST_TOPIC))
            .values()
            .get(SERVICE_TEST_TOPIC)
            .get()
            .name()).thenReturn(SERVICE_TEST_TOPIC);
        return null;
      }
    };

    Mockito.when(_topicManagementHelper._topicFactory.createTopicIfNotExist(Mockito.anyString(), Mockito.anyShort(),
        Mockito.anyDouble(), Mockito.any(), Mockito.any())).thenAnswer(createKafkaTopicFutureAnswer);

    _topicManagementHelper.maybeCreateTopic();

    Assert.assertNotNull(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(SERVICE_TEST_TOPIC))
        .values()
        .get(SERVICE_TEST_TOPIC)
        .get());
    Assert.assertEquals(_topicManagementHelper._adminClient.describeTopics(Collections.singleton(SERVICE_TEST_TOPIC))
        .values()
        .get(SERVICE_TEST_TOPIC)
        .get()
        .name(), SERVICE_TEST_TOPIC);
  }
}
