/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services.configs;

import com.linkedin.xinfra.monitor.topicfactory.DefaultTopicFactory;
import com.linkedin.xinfra.monitor.topicfactory.TopicFactory;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * This class is used to extract configuration from a Map&lt;String, Object&gt;, setup defaults and perform basic bounds
 * checking on the values found in the map.  This is used by the TopicManagementService when it is constructed.
 */
public class TopicManagementServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String TOPIC_CONFIG = CommonServiceConfig.TOPIC_CONFIG;
  public static final String TOPIC_DOC = CommonServiceConfig.TOPIC_DOC;

  public static final String PARTITIONS_TO_BROKERS_RATIO_CONFIG = "topic-management.partitionsToBrokersRatio";
  public static final String PARTITIONS_TO_BROKERS_RATIO_DOC = "New partitions are added when the actual ratio falls below this threshold."
      + " This config provides a loose lower bound on the partitionNum-to-brokerNum ratio when the monitor topic is created or when partition is added.";

  public static final String MIN_PARTITION_NUM_CONFIG = "topic-management.minPartitionNum";
  public static final String MIN_PARTITION_NUM_DOC = "New partitions are added when the actual partition number falls below this threshold."
      + " This config provides a loose lower bound on the partition number of the monitor topic when the topic is created or when partition is added.";

  public static final String TOPIC_REPLICATION_FACTOR_CONFIG = "topic-management.replicationFactor";
  public static final String TOPIC_REPLICATION_FACTOR_DOC = "This replication factor is used to create the monitor topic. "
      + "The larger one of the current replication factor and the configured replication factor is used to expand partition "
      + "of the monitor topic.";

  public static final String TOPIC_CREATION_ENABLED_CONFIG = "topic-management.topicCreationEnabled";
  public static final String TOPIC_CREATION_ENABLED_DOC = String.format("When true this service automatically creates the topic named"
      + " in the config with replication factor %s and min ISR as max(%s - 1, 1). The partition number is determined based on %s and %s",
      TOPIC_REPLICATION_FACTOR_CONFIG, TOPIC_REPLICATION_FACTOR_CONFIG, PARTITIONS_TO_BROKERS_RATIO_CONFIG, MIN_PARTITION_NUM_DOC);

  public static final String TOPIC_ADD_PARTITION_ENABLED_CONFIG = "topic-management.topicAddPartitionEnabled";
  public static final String TOPIC_ADD_PARTITION_ENABLED_DOC = String.format("When true this service automatically add topic partition(s) "
      + "if the current topic partition count is smaller than the partition number which is determined based on %s and %s",
          PARTITIONS_TO_BROKERS_RATIO_CONFIG, MIN_PARTITION_NUM_DOC);

  public static final String TOPIC_REASSIGN_PARTITION_AND_ELECT_LEADER_ENABLED_CONFIG = "topic-management.topicReassignPartitionAndElectLeaderEnabled";
  public static final String TOPIC_REASSIGN_PARTITION_AND_ELECT_LEADER_ENABLED_DOC = "When true this service automatically balance topic partitions in"
      + " a cluster to ensure a minimum number of leader replicas on each alive broker.";

  public static final String TOPIC_FACTORY_CLASS_CONFIG = "topic-management.topicFactory.class.name";
  public static final String TOPIC_FACTORY_CLASS_DOC = "The name of the class used to create topics.  This class must implement "
      + TopicFactory.class.getName() + ".";

  public static final String TOPIC_FACTORY_PROPS_CONFIG = "topic-management.topicFactory.props";
  public static final String TOPIC_FACTORY_PROPS_DOC = "A configuration map for the topic factory.";

  public static final String TOPIC_PROPS_CONFIG = "topic-management.topic.props";
  public static final String TOPIC_PROPS_DOC = "A configuration map for the topic";

  public static final String TOPIC_MANAGEMENT_ENABLED_CONFIG = "topic-management.topicManagementEnabled";
  public static final String TOPIC_MANAGEMENT_ENABLED_DOC = "Boolean switch for enabling Topic Management Service";

  static {
    CONFIG = new ConfigDef()
      .define(TOPIC_MANAGEMENT_ENABLED_CONFIG,
              ConfigDef.Type.BOOLEAN,
              true,
              ConfigDef.Importance.HIGH,
              TOPIC_MANAGEMENT_ENABLED_DOC)
      .define(TOPIC_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              TOPIC_DOC)
      .define(PARTITIONS_TO_BROKERS_RATIO_CONFIG,
              ConfigDef.Type.DOUBLE,
              1.0,
              ConfigDef.Importance.LOW,
              PARTITIONS_TO_BROKERS_RATIO_DOC)
      .define(MIN_PARTITION_NUM_CONFIG,
              ConfigDef.Type.INT,
              1,
              ConfigDef.Importance.LOW,
              MIN_PARTITION_NUM_DOC)
      .define(TOPIC_CREATION_ENABLED_CONFIG,
              ConfigDef.Type.BOOLEAN,
              true,
              ConfigDef.Importance.LOW,
              TOPIC_CREATION_ENABLED_DOC)
      .define(TOPIC_ADD_PARTITION_ENABLED_CONFIG,
              ConfigDef.Type.BOOLEAN,
              true,
              ConfigDef.Importance.LOW,
              TOPIC_ADD_PARTITION_ENABLED_DOC)
      .define(TOPIC_REASSIGN_PARTITION_AND_ELECT_LEADER_ENABLED_CONFIG,
              ConfigDef.Type.BOOLEAN,
              true,
              ConfigDef.Importance.LOW,
              TOPIC_REASSIGN_PARTITION_AND_ELECT_LEADER_ENABLED_DOC)
      .define(TOPIC_REPLICATION_FACTOR_CONFIG,
              ConfigDef.Type.INT,
              1,
              atLeast(1),
              ConfigDef.Importance.LOW,
              TOPIC_REPLICATION_FACTOR_DOC)
      .define(TOPIC_FACTORY_CLASS_CONFIG,
              ConfigDef.Type.STRING,
              DefaultTopicFactory.class.getCanonicalName(),
              ConfigDef.Importance.LOW,
              TOPIC_FACTORY_CLASS_DOC);
  }

  public TopicManagementServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
