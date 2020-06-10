/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services.configs;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class GraphiteMetricsReporterServiceConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;

  public static final String REPORT_METRICS_CONFIG = CommonServiceConfig.REPORT_METRICS_CONFIG;
  public static final String REPORT_METRICS_DOC = CommonServiceConfig.REPORT_METRICS_DOC;

  public static final String REPORT_INTERVAL_SEC_CONFIG = CommonServiceConfig.REPORT_INTERVAL_SEC_CONFIG;
  public static final String REPORT_INTERVAL_SEC_DOC = CommonServiceConfig.REPORT_INTERVAL_SEC_DOC;

  public static final String REPORT_GRAPHITE_HOST = "report.graphite.host";
  public static final String REPORT_GRAPHITE_HOST_DOC = "The host of graphite server which GraphiteMetricsReporterService will report the metrics values.";

  public static final String REPORT_GRAPHITE_PORT = "report.graphite.port";
  public static final String REPORT_GRAPHITE_PORT_DOC = "The port of graphite server which GraphiteMetricsReporterService will report the metrics values.";

  public static final String REPORT_GRAPHITE_PREFIX = "report.graphite.prefix";
  public static final String REPORT_GRAPHITE_PREFIX_DOC = "The prefix of graphite metric name that will be generated with metric name to report to graphite server.";

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
                            .define(REPORT_GRAPHITE_HOST,
                                    ConfigDef.Type.STRING,
                                    "localhost",
                                    ConfigDef.Importance.MEDIUM,
                                    REPORT_GRAPHITE_HOST_DOC)
                            .define(REPORT_GRAPHITE_PORT,
                                    ConfigDef.Type.INT,
                                    2003,
                                    ConfigDef.Importance.MEDIUM,
                                    REPORT_GRAPHITE_PORT_DOC)
                            .define(REPORT_GRAPHITE_PREFIX,
                                    ConfigDef.Type.STRING,
                                    "",
                                    ConfigDef.Importance.LOW,
                                    REPORT_GRAPHITE_PREFIX_DOC);

  }

  public GraphiteMetricsReporterServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
