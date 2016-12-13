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

import org.apache.kafka.clients.CommonClientConfigs;

public class CommonServiceConfig {

  public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
  public static final String ZOOKEEPER_CONNECT_DOC = "Zookeeper connect string.";

  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = CommonClientConfigs.BOOSTRAP_SERVERS_DOC;

  public static final String TOPIC_CONFIG = "topic";
  public static final String TOPIC_DOC = "Topic to be used by the service.";

  public static final String PARTITIONS_TO_BROKER_RATO_CONFIG = "topic-management.partitionsToBrokersRatio";
  public static final String PARTITIONS_TO_BROKER_RATIO_DOC = "Determines the number of partitions per broker when a topic is"
    + " created or rebalanced.  ceil(nBrokers * partitionsToBrokerRatio) is used to determine the actual number of "
    + "partitions when partitions are added or removed.";

  public static final String TOPIC_REPLICATION_FACTOR_CONFIG = "topic-management.replicationFactor";
  public static final String TOPIC_REPLICATION_FACTOR_DOC = "When a topic is created automatically this is the "
      + "replication factor used.";

  public static final String TOPIC_CREATION_ENABLED_CONFIG = "topic-management.topicCreationEnabled";
  public static final String TOPIC_CREATION_ENABLED_DOC = "When true this automatically creates the topic mentioned by \"" +
      TOPIC_CONFIG + "\" with replication factor \"" + TOPIC_REPLICATION_FACTOR_CONFIG + "and min ISR of max(" +
      TOPIC_REPLICATION_FACTOR_CONFIG + "-1, 1) with number of brokers * \"" + PARTITIONS_TO_BROKER_RATO_CONFIG + "\" partitions.";

  public static final String TOPIC_FACTORY_CONFIG = "topic-management.topicFactory.class.name";
  public static final String TOPIC_FACTORY_DOC = "The name of the class used to create topics.  This class must implement "
      + com.linkedin.kmf.common.TopicFactory.class.getName() + ".";

}