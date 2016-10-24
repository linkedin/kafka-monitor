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

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class TopicManagementConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String ZOOKEEPER_CONNECT_CONFIG = CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG;
  public static final String ZOOKEEPER_CONNECT_DOC = CommonServiceConfig.ZOOKEEPER_CONNECT_DOC;

  public static final String TOPIC_CONFIG = CommonServiceConfig.TOPIC_CONFIG;
  public static final String TOPIC_DOC = CommonServiceConfig.TOPIC_DOC;

  public static final String PARTITIONS_PER_BROKER_THRESHOLD = "topic-management.partitionBrokerRatioThreshold";
  public static final String PARTITIONS_PER_BROKER_THRESHOLD_DOC = "The expected ratio of partition / broker.  When the"
    + " actual ratio falls below this threshold new partitions are created.";

  public static final String REBALANCE_INTERVAL_MS_CONFIG = "topic-management.rebalance.interval.ms";
  public static final String REBALANCE_INTERVAL_MS_DOC = "The gap in ms between the times the cluster balance on the "
    + "monitored topic is checked.  Set this to a large value to disable automatic topic rebalance.";

  public static final String PARTITIONS_PER_BROKER_CONFIG = ProduceServiceConfig.PARTITIONS_PER_BROKER_CONFIG;
  public static final String PARTITIONS_PER_BROKER_DOC = ProduceServiceConfig.PARTITIONS_PER_BROKER_DOC;

  static {
    CONFIG = new ConfigDef()
      .define(ZOOKEEPER_CONNECT_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        ZOOKEEPER_CONNECT_DOC)
      .define(TOPIC_CONFIG,
        ConfigDef.Type.STRING,
        ProduceServiceConfig.TOPIC_CONFIG_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        TOPIC_DOC)
      .define(PARTITIONS_PER_BROKER_THRESHOLD,
        ConfigDef.Type.DOUBLE, 1.0,
        atLeast(1.0),
        ConfigDef.Importance.LOW, PARTITIONS_PER_BROKER_THRESHOLD_DOC)
      .define(REBALANCE_INTERVAL_MS_CONFIG,
        ConfigDef.Type.INT,
        1000 * 60 * 10,
        atLeast(10),
        ConfigDef.Importance.LOW, REBALANCE_INTERVAL_MS_DOC)
      .define(PARTITIONS_PER_BROKER_CONFIG,
        ConfigDef.Type.INT,
        2,
        atLeast(1),
        ConfigDef.Importance.LOW, PARTITIONS_PER_BROKER_DOC);

  }

  public TopicManagementConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
