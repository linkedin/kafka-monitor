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

public class DefaultMetricsReporterServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String REPORT_METRICS_CONFIG = "report.metrics.list";
  public static final String REPORT_METRICS_DOC = "A list of objectName/attributeName pairs used to filter the metrics "
                                                        + "that will be exported. Only metrics that match any pair in the list will be exported. "
                                                        + "Each pair is in the form <code>objectName:attributeName<code>, where objectName and "
                                                        + "attributeName can contain wild card. If no objectName/attributeName is specified, "
                                                        + "all metrics with JMX prefix kmf.services will be reported";

  public static final String REPORT_INTERVAL_SEC_CONFIG = "report.interval.sec";
  public static final String REPORT_INTERVAL_SEC_DOC = "The interval in second by which DefaultMetricsReporterService will report the metrics values.";

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
                                    REPORT_INTERVAL_SEC_DOC);

  }

  public DefaultMetricsReporterServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
