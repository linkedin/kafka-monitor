/*
 * Copyright (C) 2018 SignalFx, Inc. Licensed under the Apache 2 License.
 */

package com.linkedin.kmf.services.configs;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * key/value pair used for configuring SignalFxMetricsReporterService
 *
 */
public class SignalFxMetricsReporterServiceConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;

  public static final String REPORT_METRICS_CONFIG = "report.metrics.list";
  public static final String REPORT_METRICS_DOC = CommonServiceConfig.REPORT_METRICS_DOC;

  public static final String REPORT_INTERVAL_SEC_CONFIG = CommonServiceConfig.REPORT_INTERVAL_SEC_CONFIG;
  public static final String REPORT_INTERVAL_SEC_DOC = CommonServiceConfig.REPORT_INTERVAL_SEC_DOC;

  public static final String REPORT_SIGNALFX_URL = "report.signalfx.url";
  public static final String REPORT_SIGNALFX_URL_DOC = "The url of signalfx server which SignalFxMetricsReporterService will report the metrics values.";

  public static final String SIGNALFX_METRIC_DIMENSION = "report.metric.dimensions";
  public static final String SIGNALFX_METRIC_DIMENSION_DOC = "Dimensions added to each metric. Example: {\"key1:value1\", \"key2:value2\"} ";

  public static final String SIGNALFX_TOKEN = "report.signalfx.token";
  public static final String SIGNALFX_TOKEN_DOC = "SignalFx access token";

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
                             .define(REPORT_SIGNALFX_URL,
                                    ConfigDef.Type.STRING,
                                    "",
                                    ConfigDef.Importance.LOW,
                                    REPORT_SIGNALFX_URL_DOC)
                             .define(SIGNALFX_TOKEN,
                                    ConfigDef.Type.STRING,
                                    "",
                                    ConfigDef.Importance.HIGH,
                                    SIGNALFX_TOKEN_DOC);
  }

  public SignalFxMetricsReporterServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}

