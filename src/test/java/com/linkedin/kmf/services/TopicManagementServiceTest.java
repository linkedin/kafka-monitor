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

import com.linkedin.kmf.services.MultiClusterTopicManagementService.TopicManagementHelper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TopicManagementServiceTest {

  private static final String TOPIC = "kmf-unit-test-topic";

  private List<Node> brokers(int brokerCount) {
    List<Node> brokers = new ArrayList<>();
    for (int i = 0; i < brokerCount; i++) {
      brokers.add(new Node(i, "", -1));
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
    List<TopicPartitionInfo> partitions = new ArrayList<>();
    Node[] node = nodes(2);
    partitions.add(new TopicPartitionInfo(0, node[0], new ArrayList<>(Arrays.asList(node[0], node[1])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(1, node[0], new ArrayList<>(Arrays.asList(node[0], node[1])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(2, node[1], new ArrayList<>(Arrays.asList(node[1], node[0])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(3, node[1], new ArrayList<>(Arrays.asList(node[1], node[0])), new ArrayList<>()));

    Assert.assertFalse(TopicManagementHelper.someBrokerNotPreferredLeader(partitions, brokers(2)));
    Assert.assertFalse(TopicManagementHelper.someBrokerNotElectedLeader(partitions, brokers(2)));
  }

  @Test
  public void detectLowTotalNumberOfPartitions() {
    List<TopicPartitionInfo> partitions = new ArrayList<>();
    Node[] node = nodes(3);
    partitions.add(new TopicPartitionInfo(0, node[0], new ArrayList<>(Arrays.asList(node[0], node[1])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(1, node[1], new ArrayList<>(Arrays.asList(node[1], node[0])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(2, node[2], new ArrayList<>(Arrays.asList(node[2], node[0])), new ArrayList<>()));
    Assert.assertFalse(TopicManagementHelper.someBrokerNotPreferredLeader(partitions, brokers(3)));
    Assert.assertFalse(TopicManagementHelper.someBrokerNotElectedLeader(partitions, brokers(3)));
    Assert.assertEquals(TopicManagementHelper.getReplicationFactor(partitions), 2);
  }


  @Test
  public void detectBrokerWithoutLeader() {
    List<TopicPartitionInfo> partitions = new ArrayList<>();
    Node[] node = nodes(3);
    partitions.add(new TopicPartitionInfo(0, node[0], new ArrayList<>(Arrays.asList(node[0], node[1])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(1, node[0], new ArrayList<>(Arrays.asList(node[0], node[1])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(2, node[1], new ArrayList<>(Arrays.asList(node[1], node[0])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(3, node[1], new ArrayList<>(Arrays.asList(node[2], node[1])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(4, node[1], new ArrayList<>(Arrays.asList(node[2], node[0])), new ArrayList<>()));

    Assert.assertFalse(TopicManagementHelper.someBrokerNotPreferredLeader(partitions, brokers(3)));
    Assert.assertTrue(TopicManagementHelper.someBrokerNotElectedLeader(partitions, brokers(3)));
  }

  @Test
  public void detectBrokerWithoutPreferredLeader() {
    List<TopicPartitionInfo> partitions = new ArrayList<>();
    Node[] node = nodes(3);
    partitions.add(new TopicPartitionInfo(0, node[0], new ArrayList<>(Arrays.asList(node[0], node[1])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(1, node[0], new ArrayList<>(Arrays.asList(node[0], node[1])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(2, node[1], new ArrayList<>(Arrays.asList(node[0], node[0])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(3, node[1], new ArrayList<>(Arrays.asList(node[2], node[1])), new ArrayList<>()));
    partitions.add(new TopicPartitionInfo(4, node[1], new ArrayList<>(Arrays.asList(node[2], node[0])), new ArrayList<>()));

    Assert.assertTrue(TopicManagementHelper.someBrokerNotPreferredLeader(partitions, brokers(3)));
    Assert.assertTrue(TopicManagementHelper.someBrokerNotElectedLeader(partitions, brokers(3)));
  }
}
