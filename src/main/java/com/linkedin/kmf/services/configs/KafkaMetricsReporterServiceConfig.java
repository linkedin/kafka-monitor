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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2018/01/18
 * Time: 10:38 AM
 * Desc:
 */
public class KafkaMetricsReporterServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String REPORT_METRICS_CONFIG = CommonServiceConfig.REPORT_METRICS_CONFIG;
  public static final String REPORT_METRICS_DOC = CommonServiceConfig.REPORT_METRICS_DOC;

  public static final String REPORT_INTERVAL_SEC_CONFIG = CommonServiceConfig.REPORT_INTERVAL_SEC_CONFIG;
  public static final String REPORT_INTERVAL_SEC_DOC = CommonServiceConfig.REPORT_INTERVAL_SEC_DOC;

  public static final String ZOOKEEPER_CONNECT_CONFIG = CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG;
  public static final String ZOOKEEPER_CONNECT_DOC = CommonServiceConfig.ZOOKEEPER_CONNECT_DOC;

  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonServiceConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = CommonServiceConfig.BOOTSTRAP_SERVERS_DOC;

  public static final String TOPIC_CONFIG = CommonServiceConfig.TOPIC_CONFIG;
  public static final String TOPIC_DOC = CommonServiceConfig.TOPIC_DOC;

  public static final String PRODUCER_ID_CONFIG = "produce.producer.id";
  public static final String PRODUCER_ID_DOC = "Client id that will be used in the record sent by produce service.";


  static {
    CONFIG = new ConfigDef().define(REPORT_METRICS_CONFIG,
      ConfigDef.Type.LIST,
      Arrays.asList("kmf.services:*:*"),
      ConfigDef.Importance.MEDIUM,
      REPORT_METRICS_DOC)
      .define(REPORT_INTERVAL_SEC_CONFIG,
        ConfigDef.Type.INT,
        1,
        ConfigDef.Importance.LOW,
        REPORT_INTERVAL_SEC_DOC)
      .define(ZOOKEEPER_CONNECT_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        ZOOKEEPER_CONNECT_DOC)
      .define(BOOTSTRAP_SERVERS_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        BOOTSTRAP_SERVERS_DOC)
      .define(TOPIC_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        TOPIC_DOC)
      .define(PRODUCER_ID_CONFIG,
        ConfigDef.Type.STRING,
        "kmf-producer-metrics",
        ConfigDef.Importance.LOW,
        PRODUCER_ID_DOC);
  }

  public KafkaMetricsReporterServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
