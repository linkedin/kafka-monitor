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

  public static final String ZOOKEEPER_CONNECT_CONFIG = CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG;
  public static final String ZOOKEEPER_CONNECT_DOC = CommonServiceConfig.ZOOKEEPER_CONNECT_DOC;

  public static final String TOPIC_CONFIG = CommonServiceConfig.TOPIC_CONFIG;
  public static final String TOPIC_DOC = CommonServiceConfig.TOPIC_DOC;

  public static final String PARTITIONS_TO_BROKERS_RATIO_CONFIG = "topic-management.partitionsToBrokersRatio";
  public static final String PARTITIONS_TO_BROKERS_RATIO_DOC = "New partitions are created when the actual ratio falls below this threshold."
      + " partitionsToBrokerRatio will be the lower bound on the number of partitions per broker when a topic is"
      + " created or when the partition is added.";

  public static final String TOPIC_REPLICATION_FACTOR_CONFIG = "topic-management.replicationFactor";
  public static final String TOPIC_REPLICATION_FACTOR_DOC = "When a topic is created automatically this is the "
      + "replication factor used.";

  public static final String TOPIC_CREATION_ENABLED_CONFIG = "topic-management.topicCreationEnabled";
  public static final String TOPIC_CREATION_ENABLED_DOC = "When true this automatically creates the topic mentioned by \""
    + CommonServiceConfig.TOPIC_CONFIG + "\" with replication factor \"" + TOPIC_REPLICATION_FACTOR_CONFIG + "and min ISR of max("
    + TOPIC_REPLICATION_FACTOR_CONFIG + "-1, 1) with number of brokers * \"" + PARTITIONS_TO_BROKERS_RATIO_CONFIG + "\" partitions.";

  public static final String TOPIC_FACTORY_CLASS_CONFIG = "topic-management.topicFactory.class.name";
  public static final String TOPIC_FACTORY_CLASS_DOC = "The name of the class used to create topics.  This class must implement "
      + TopicFactory.class.getName() + ".";

  public static final String TOPIC_FACTORY_PROPS_CONFIG = "topic-management.topicFactory.props";
  public static final String TOPIC_FACTORY_PROPS_DOC = "A configuration map for the topic factory.";

  static {
    CONFIG = new ConfigDef()
      .define(ZOOKEEPER_CONNECT_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              ZOOKEEPER_CONNECT_DOC)
      .define(TOPIC_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              TOPIC_DOC)
      .define(PARTITIONS_TO_BROKERS_RATIO_CONFIG,
              ConfigDef.Type.DOUBLE,
              1.5,
              ConfigDef.Importance.LOW, PARTITIONS_TO_BROKERS_RATIO_DOC)
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
