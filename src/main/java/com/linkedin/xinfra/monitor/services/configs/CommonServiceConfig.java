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

import org.apache.kafka.clients.CommonClientConfigs;

public class CommonServiceConfig {

  public static final String CONSUMER_PROPS_CONFIG = "consumer.props";
  public static final String CONSUMER_PROPS_DOC = "consumer props";

  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = CommonClientConfigs.BOOTSTRAP_SERVERS_DOC;

  public static final String TOPIC_CONFIG = "topic";
  public static final String TOPIC_DOC = "Topic to be used by the service.";

  public static final String REPORT_METRICS_CONFIG = "report.metrics.list";
  public static final String REPORT_METRICS_DOC = "A list of objectName/attributeName pairs used to filter the metrics "
      + "that will be exported. Only metrics that match any pair in the list will be exported. "
      + "Each pair is in the form <code>objectName:attributeName<code>, where objectName and "
      + "attributeName can contain wild card. If no objectName/attributeName is specified, "
      + "all metrics with JMX prefix kmf.services will be reported";

  public static final String REPORT_INTERVAL_SEC_CONFIG = "report.interval.sec";
  public static final String REPORT_INTERVAL_SEC_DOC = "The interval in second by which metrics reporter service will report the metrics values.";
}
