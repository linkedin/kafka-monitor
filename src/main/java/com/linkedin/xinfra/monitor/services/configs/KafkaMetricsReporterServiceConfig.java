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

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class KafkaMetricsReporterServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String REPORT_METRICS_CONFIG = CommonServiceConfig.REPORT_METRICS_CONFIG;
  public static final String REPORT_METRICS_DOC = CommonServiceConfig.REPORT_METRICS_DOC;

  public static final String REPORT_INTERVAL_SEC_CONFIG = CommonServiceConfig.REPORT_INTERVAL_SEC_CONFIG;
  public static final String REPORT_INTERVAL_SEC_DOC = CommonServiceConfig.REPORT_INTERVAL_SEC_DOC;

  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonServiceConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = CommonServiceConfig.BOOTSTRAP_SERVERS_DOC;

  public static final String TOPIC_CONFIG = CommonServiceConfig.TOPIC_CONFIG;
  public static final String TOPIC_DOC = CommonServiceConfig.TOPIC_DOC;

  public static final String TOPIC_REPLICATION_FACTOR = "report.kafka.topic.replication.factor";
  public static final String TOPIC_REPLICATION_FACTOR_DOC = "This replication factor is used to create the metrics reporter topic.";


  static {
    CONFIG = new ConfigDef().define(REPORT_METRICS_CONFIG,
                                    ConfigDef.Type.LIST, Collections.singletonList("kmf.services:*:*"),
                                    ConfigDef.Importance.MEDIUM,
                                    REPORT_METRICS_DOC)
                            .define(REPORT_INTERVAL_SEC_CONFIG,
                                    ConfigDef.Type.INT,
                                    1,
                                    ConfigDef.Importance.LOW,
                                    REPORT_INTERVAL_SEC_DOC)
                            .define(BOOTSTRAP_SERVERS_CONFIG,
                                    ConfigDef.Type.STRING,
                                    ConfigDef.Importance.HIGH,
                                    BOOTSTRAP_SERVERS_DOC)
                            .define(TOPIC_CONFIG,
                                    ConfigDef.Type.STRING,
                                    ConfigDef.Importance.HIGH,
                                    TOPIC_DOC)
                            .define(TOPIC_REPLICATION_FACTOR,
                                    ConfigDef.Type.INT,
                                    1,
                                    atLeast(1),
                                    ConfigDef.Importance.LOW,
                                    TOPIC_REPLICATION_FACTOR_DOC);
  }

  public KafkaMetricsReporterServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
