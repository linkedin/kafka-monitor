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

  static {
    CONFIG = new ConfigDef()
      .define(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        CommonServiceConfig.ZOOKEEPER_CONNECT_DOC)
      .define(CommonServiceConfig.TOPIC_CONFIG,
        ConfigDef.Type.STRING,
        "kmf-producer",
        ConfigDef.Importance.MEDIUM,
        CommonServiceConfig.TOPIC_DOC)
      .define(PARTITIONS_TO_BROKER_RATIO_THRESHOLD,
        ConfigDef.Type.DOUBLE,
        1.0,
        atLeast(1.0),
        ConfigDef.Importance.LOW,
        PARTITIONS_TO_BROKER_RATIO_THRESHOLD_DOC)
      .define(REBALANCE_INTERVAL_MS_CONFIG,
        ConfigDef.Type.INT,
        1000 * 60 * 10,
        atLeast(10),
        ConfigDef.Importance.LOW, REBALANCE_INTERVAL_MS_DOC)
      .define(CommonServiceConfig.PARTITIONS_TO_BROKER_RATO_CONFIG,
        ConfigDef.Type.DOUBLE,
        2.0,
        atLeast(1),
        ConfigDef.Importance.LOW,
        CommonServiceConfig.PARTITIONS_TO_BROKER_RATIO_DOC);
  }

  public TopicManagementServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
