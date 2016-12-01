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

import com.linkedin.kmf.consumer.NewConsumer;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;


public class ConsumeServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String ZOOKEEPER_CONNECT_CONFIG = CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG;
  public static final String ZOOKEEPER_CONNECT_DOC = CommonServiceConfig.ZOOKEEPER_CONNECT_DOC;

  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonServiceConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = CommonServiceConfig.BOOTSTRAP_SERVERS_DOC;

  public static final String TOPIC_CONFIG = CommonServiceConfig.TOPIC_CONFIG;
  public static final String TOPIC_DOC = CommonServiceConfig.TOPIC_DOC;

  public static final String CONSUMER_CLASS_CONFIG = "consume.consumer.class";
  public static final String CONSUMER_CLASS_DOC = "Consumer class that will be instantiated as consumer in the consume service. "
    + "It can be NewConsumer, OldConsumer, or full class name of any class that implements the KMBaseConsumer interface.";

  public static final String LATENCY_PERCENTILE_MAX_MS_CONFIG = "consume.latency.percentile.max.ms";
  public static final String LATENCY_PERCENTILE_MAX_MS_DOC = "This is used to derive the bucket number used to configure latency percentile metric. "
                                                             + "Any latency larger than this max value will be rounded down to the max value.";

  public static final String LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG = "consume.latency.percentile.granularity.ms";
  public static final String LATENCY_PERCENTILE_GRANULARITY_MS_DOC = "This is used to derive the bucket number used to configure latency percentile metric. "
                                                                     + "The latency at the specified percentile should be multiple of this value.";

  public static final String CONSUMER_PROPS_CONFIG = "consume.consumer.props";
  public static final String CONSUMER_PROPS_DOC = "The properties used to config consumer in consume service.";

  public static final String TOPIC_REPLICATION_FACTOR_CONFIG = "topic-management.replicationFactor";
  public static final String TOPIC_REPLICATION_FACTOR_DOC = "When a topic is created automatically this is the "
      + "replication factor used.";

  public static final String LATENCY_SLA_MS_CONFIG = "consume.latency.sla.ms";
  public static final String LATENCY_SLA_MS_DOC = "The maximum latency of message delivery under SLA. Consume availability is measured "
                                                  + "as the fraction of messages that are either lost or whose delivery latency exceeds this value";

  public static final String TOPIC_CREATION_ENABLED_CONFIG = "consume.topic.topicCreationEnabled";
  public static final String TOPIC_CREATION_ENABLED_DOC = "When true this automatically creates the topic mentioned by \"" +
      TOPIC_CONFIG + "\" with replication factor \"" + TOPIC_REPLICATION_FACTOR_CONFIG
      + "and min ISR of max(" + TOPIC_REPLICATION_FACTOR_CONFIG + "-1, 1) with number of brokers * \"" +
      CommonServiceConfig.PARTITIONS_TO_BROKER_RATO_CONFIG + "\" partitions.";
  static {
    CONFIG = new ConfigDef().define(ZOOKEEPER_CONNECT_CONFIG,
                                    ConfigDef.Type.STRING,
                                    ConfigDef.Importance.HIGH,
                                    ZOOKEEPER_CONNECT_DOC)
                            .define(BOOTSTRAP_SERVERS_CONFIG,
                                    ConfigDef.Type.STRING,
                                    ConfigDef.Importance.HIGH,
                                    BOOTSTRAP_SERVERS_DOC)
                            .define(TOPIC_CONFIG,
                                    ConfigDef.Type.STRING,
                                    "kafka-monitor-topic",
                                    ConfigDef.Importance.LOW,
                                    TOPIC_DOC)
                            .define(TOPIC_CREATION_ENABLED_CONFIG,
                                ConfigDef.Type.BOOLEAN,
                                true,
                                ConfigDef.Importance.MEDIUM,
                                TOPIC_CREATION_ENABLED_DOC)
                            .define(CONSUMER_CLASS_CONFIG,
                                    ConfigDef.Type.STRING,
                                    NewConsumer.class.getCanonicalName(),
                                    ConfigDef.Importance.LOW,
                                    CONSUMER_CLASS_DOC)
                            .define(LATENCY_PERCENTILE_MAX_MS_CONFIG,
                                    ConfigDef.Type.INT,
                                    5000,
                                    ConfigDef.Importance.LOW,
                                    LATENCY_PERCENTILE_MAX_MS_DOC)
                            .define(LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG,
                                    ConfigDef.Type.INT,
                                    1,
                                    ConfigDef.Importance.LOW,
                                    LATENCY_PERCENTILE_GRANULARITY_MS_DOC)
                            .define(LATENCY_SLA_MS_CONFIG,
                                    ConfigDef.Type.INT,
                                    20000,
                                    ConfigDef.Importance.MEDIUM,
                                    LATENCY_SLA_MS_DOC)
                            .define(TOPIC_REPLICATION_FACTOR_CONFIG,
                                ConfigDef.Type.INT,
                                1,
                                atLeast(1),
                                ConfigDef.Importance.LOW,
                                TOPIC_REPLICATION_FACTOR_DOC);

  }

  public ConsumeServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
