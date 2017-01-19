/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.services.configs;

import com.linkedin.kmf.topicfactory.DefaultTopicFactory;
import com.linkedin.kmf.topicfactory.TopicFactory;
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

  public static final String PARTITIONS_TO_BROKER_RATIO_THRESHOLD = "topic-management.partitionsToBrokersRatioThreshold";
  public static final String PARTITIONS_TO_BROKER_RATIO_THRESHOLD_DOC = "The expected ratio of partitions / brokers.  When the"
    + " actual ratio falls below this threshold new partitions are created.";

  public static final String REBALANCE_INTERVAL_MS_CONFIG = "topic-management.rebalance.interval.ms";
  public static final String REBALANCE_INTERVAL_MS_DOC = "The gap in ms between the times the cluster balance on the "
    + "monitored topic is checked.  Set this to a large value to disable automatic topic rebalance.";

  public static final String PARTITIONS_TO_BROKER_RATIO_CONFIG = "topic-management.partitionsToBrokersRatio";
  public static final String PARTITIONS_TO_BROKER_RATIO_DOC = "Determines the number of partitions per broker when a topic is"
      + " created or rebalanced.  ceil(nBrokers * partitionsToBrokerRatio) is used to determine the actual number of "
      + "partitions when partitions are added or removed.";

  public static final String TOPIC_REPLICATION_FACTOR_CONFIG = "topic-management.replicationFactor";
  public static final String TOPIC_REPLICATION_FACTOR_DOC = "When a topic is created automatically this is the "
      + "replication factor used.";

  public static final String TOPIC_CREATION_ENABLED_CONFIG = "topic-management.topicCreationEnabled";
  public static final String TOPIC_CREATION_ENABLED_DOC = "When true this automatically creates the topic mentioned by \""
    + CommonServiceConfig.TOPIC_CONFIG + "\" with replication factor \"" + TOPIC_REPLICATION_FACTOR_CONFIG + "and min ISR of max("
    + TOPIC_REPLICATION_FACTOR_CONFIG + "-1, 1) with number of brokers * \"" + PARTITIONS_TO_BROKER_RATIO_CONFIG + "\" partitions.";

  public static final String TOPIC_FACTORY_CONFIG = "topic-management.topicFactory.class.name";
  public static final String TOPIC_FACTORY_DOC = "The name of the class used to create topics.  This class must implement "
      + TopicFactory.class.getName() + ".";

  static {
    CONFIG = new ConfigDef()
      .define(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              CommonServiceConfig.ZOOKEEPER_CONNECT_DOC)
      .define(CommonServiceConfig.TOPIC_CONFIG,
              ConfigDef.Type.STRING,
              "kafka-monitor-topic",
              ConfigDef.Importance.MEDIUM,
              CommonServiceConfig.TOPIC_DOC)
      .define(PARTITIONS_TO_BROKER_RATIO_THRESHOLD,
              ConfigDef.Type.DOUBLE,
              1.5,
              atLeast(1.0),
              ConfigDef.Importance.LOW,
              PARTITIONS_TO_BROKER_RATIO_THRESHOLD_DOC)
      .define(REBALANCE_INTERVAL_MS_CONFIG,
              ConfigDef.Type.INT,
              1000 * 60 * 10,
              atLeast(10),
              ConfigDef.Importance.LOW,
              REBALANCE_INTERVAL_MS_DOC)
      .define(PARTITIONS_TO_BROKER_RATIO_CONFIG,
              ConfigDef.Type.DOUBLE,
              2.0,
              atLeast(1),
              ConfigDef.Importance.LOW,
              PARTITIONS_TO_BROKER_RATIO_DOC)
      .define(TOPIC_CREATION_ENABLED_CONFIG,
              ConfigDef.Type.BOOLEAN,
              true,
              ConfigDef.Importance.LOW,
              TOPIC_CREATION_ENABLED_DOC)
      .define(TOPIC_REPLICATION_FACTOR_CONFIG,
              ConfigDef.Type.INT,
              1,
              atLeast(1),
              ConfigDef.Importance.LOW,
              TOPIC_REPLICATION_FACTOR_DOC)
      .define(TOPIC_FACTORY_CONFIG,
              ConfigDef.Type.STRING,
              DefaultTopicFactory.class.getCanonicalName(),
              ConfigDef.Importance.LOW,
              TOPIC_FACTORY_DOC);
  }

  public TopicManagementServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
