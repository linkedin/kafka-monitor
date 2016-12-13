/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.services;

import java.util.ArrayList;
import java.util.List;
import kafka.cluster.Broker;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.kmf.services.TopicManagementService.TopicState;

@Test
public class TopicManagementServiceTest {

  private static final String TOPIC = "kmf-unit-test-topic";

  private List<Broker> brokers(int brokerCount) {
    List<Broker> brokers = new ArrayList<>();
    for (int i = 0; i < brokerCount; i++) {
      brokers.add(new Broker(i, "", -1, null));
    }
    return brokers;
  }

  private Node[] nodes(int brokerCount) {
    Node[] nodes = new Node[brokerCount];
    for (int i = 0; i < brokerCount; i++) {
      nodes[i] = new Node(i, "", -1);
    }
    return nodes;
  }

  @Test
  public void noDetection() {
    List<PartitionInfo> partitions = new ArrayList<>();
    Node[] node = nodes(2);
    partitions.add(new PartitionInfo(TOPIC, 0, node[0], new Node[] {node[0], node[1]}, null));
    partitions.add(new PartitionInfo(TOPIC, 1, node[0], new Node[] {node[0], node[1]}, null));
    partitions.add(new PartitionInfo(TOPIC, 2, node[1], new Node[] {node[1], node[0]}, null));
    partitions.add(new PartitionInfo(TOPIC, 3, node[1], new Node[] {node[1], node[0]}, null));

    TopicState topicState = TopicManagementService.topicState(partitions, brokers(2), 1.1);
    Assert.assertFalse(topicState.someBrokerMissingPartition());
    Assert.assertFalse(topicState.someBrokerWithoutLeader());
    Assert.assertFalse(topicState.insufficientPartitions());
  }

  @Test
  public void detectLowTotalNumberOfPartitions() {
    List<PartitionInfo> partitions = new ArrayList<>();
    Node[] node = nodes(3);
    partitions.add(new PartitionInfo(TOPIC, 0, node[0], new Node[] {node[0], node[1]}, null));
    partitions.add(new PartitionInfo(TOPIC, 1, node[1], new Node[] {node[1], node[0]}, null));
    partitions.add(new PartitionInfo(TOPIC, 2, node[2], new Node[] {node[2], node[0]}, null));

    TopicState topicState = TopicManagementService.topicState(partitions, brokers(3), 1.4);
    Assert.assertFalse(topicState.someBrokerMissingPartition());
    Assert.assertFalse(topicState.someBrokerWithoutLeader());
    Assert.assertTrue(topicState.insufficientPartitions());
    Assert.assertEquals(topicState.currentReplicationFactor(), 2);
  }


  @Test
  public void detectBrokerWithoutLeader() {
    List<PartitionInfo> partitions = new ArrayList<>();
    Node[] node = nodes(3);
    partitions.add(new PartitionInfo(TOPIC, 0, node[0], new Node[] {node[0], node[1]}, null));
    partitions.add(new PartitionInfo(TOPIC, 1, node[0], new Node[] {node[0], node[1]}, null));
    partitions.add(new PartitionInfo(TOPIC, 2, node[1], new Node[] {node[1], node[0]}, null));
    partitions.add(new PartitionInfo(TOPIC, 3, node[1], new Node[] {node[2], node[1]}, null));
    partitions.add(new PartitionInfo(TOPIC, 4, node[1], new Node[] {node[2], node[0]}, null));

    TopicState topicState = TopicManagementService.topicState(partitions, brokers(3), 1.4);
    Assert.assertFalse(topicState.someBrokerMissingPartition());
    Assert.assertTrue(topicState.someBrokerWithoutLeader());
    Assert.assertFalse(topicState.insufficientPartitions());
  }

  @Test
  public void detectBrokerWithoutPreferredLeader() {
    List<PartitionInfo> partitions = new ArrayList<>();
    Node[] node = nodes(3);
    partitions.add(new PartitionInfo(TOPIC, 0, node[0], new Node[] {node[0], node[1]}, null));
    partitions.add(new PartitionInfo(TOPIC, 1, node[0], new Node[] {node[0], node[1]}, null));
    partitions.add(new PartitionInfo(TOPIC, 2, node[1], new Node[] {node[0], node[0]}, null));
    partitions.add(new PartitionInfo(TOPIC, 3, node[1], new Node[] {node[2], node[1]}, null));
    partitions.add(new PartitionInfo(TOPIC, 4, node[1], new Node[] {node[2], node[0]}, null));

    TopicState topicState = TopicManagementService.topicState(partitions, brokers(3), 1.4);
    Assert.assertTrue(topicState.someBrokerMissingPartition());
    Assert.assertTrue(topicState.someBrokerWithoutLeader());
    Assert.assertFalse(topicState.insufficientPartitions());
  }
}
