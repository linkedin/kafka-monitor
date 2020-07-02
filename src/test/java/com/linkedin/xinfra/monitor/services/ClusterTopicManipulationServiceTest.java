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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.linkedin.xinfra.monitor.XinfraMonitorConstants;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Testing methods for the Xinfra Monitor class of ClusterTopicManipulationService.
 */
@Test
public class ClusterTopicManipulationServiceTest {

  private static final String SERVICE_TEST_TOPIC = "xinfra-monitor-topic-manipulation-test-topic";

  @BeforeMethod
  private void startTest() {
    System.out.println("Started " + this.getClass().getSimpleName().toLowerCase() + ".");
  }

  @AfterMethod
  private void finishTest() {
    System.out.println("Finished " + this.getClass().getCanonicalName().toLowerCase() + ".");
  }

  @Test(invocationCount = 2)
  void serviceStartTest() throws JsonProcessingException {
    ClusterTopicManipulationService clusterTopicManipulationService =
        Mockito.mock(ClusterTopicManipulationService.class);

    Mockito.doCallRealMethod()
        .when(clusterTopicManipulationService)
        .processBroker(Mockito.anyMap(), Mockito.any(), Mockito.anyString());

    Mockito.doCallRealMethod()
        .when(clusterTopicManipulationService)
        .setExpectedPartitionsCount(Mockito.anyInt());

    Mockito.doCallRealMethod()
        .when(clusterTopicManipulationService)
        .expectedPartitionsCount();

    List<Node> brokers = new ArrayList<>();
    for (int id = 1; id < 3; id++) {
      brokers.add(new Node(id, "kafka-broker-host", 8000));
    }

    Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> logDirectoriesResponseMap1 = new HashMap<>();
    Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> logDirectoriesResponseMap2 = new HashMap<>();

    Map<Node, Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>>> brokerMapHashMap = new HashMap<>();
    brokerMapHashMap.putIfAbsent(brokers.get(0), logDirectoriesResponseMap1);
    brokerMapHashMap.putIfAbsent(brokers.get(1), logDirectoriesResponseMap2);

    Map<String, DescribeLogDirsResponse.LogDirInfo> logDirInfoMap1 = new HashMap<>();
    Map<String, DescribeLogDirsResponse.LogDirInfo> logDirInfoMap2 = new HashMap<>();

    logDirectoriesResponseMap1.put(brokers.get(0).id(), logDirInfoMap1);
    logDirectoriesResponseMap2.put(brokers.get(1).id(), logDirInfoMap2);

    Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicaInfos1 = new HashMap<>();
    Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicaInfos2 = new HashMap<>();

    for (int topicPartition = 0; topicPartition < 3; topicPartition++) {
      replicaInfos1.put(new TopicPartition(SERVICE_TEST_TOPIC, topicPartition),
          new DescribeLogDirsResponse.ReplicaInfo(235, 0, false));

      replicaInfos2.put(new TopicPartition(SERVICE_TEST_TOPIC, topicPartition),
          new DescribeLogDirsResponse.ReplicaInfo(235, 0, false));
    }

    int totalPartitions = brokers.size() * replicaInfos1.size();
    System.out.println(totalPartitions);
    clusterTopicManipulationService.setExpectedPartitionsCount(totalPartitions);
    System.out.println(clusterTopicManipulationService.expectedPartitionsCount());

    logDirInfoMap1.put(XinfraMonitorConstants.KAFKA_LOG_DIRECTORY + "-1",
        new DescribeLogDirsResponse.LogDirInfo(null, replicaInfos1));
    logDirInfoMap2.put(XinfraMonitorConstants.KAFKA_LOG_DIRECTORY + "-2",
        new DescribeLogDirsResponse.LogDirInfo(null, replicaInfos2));

    ObjectMapper objectMapper = new ObjectMapper();
    ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();

    for (Map.Entry<Node, Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>>> nodeMapEntry : brokerMapHashMap.entrySet()) {
      System.out.println(objectWriter.writeValueAsString(nodeMapEntry.getValue()));
    }

    for (Node broker : brokers) {
      clusterTopicManipulationService.processBroker(brokerMapHashMap.get(broker), broker, SERVICE_TEST_TOPIC);
    }

    Assert.assertEquals(totalPartitions, clusterTopicManipulationService.expectedPartitionsCount());
    System.out.println();
  }
}

