/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.services;

import com.linkedin.kmf.consumer.OldConsumer;
import com.linkedin.kmf.producer.NewProducer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import java.util.Map;

public class ServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  // common configs
  public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
  public static final String ZOOKEEPER_CONNECT_DOC = "Zookeeper connect string.";

  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = CommonClientConfigs.BOOSTRAP_SERVERS_DOC;

  public static final String TOPIC_CONFIG = "topic";
  public static final String TOPIC_DOC = "Topic that the service will produce to and consume from.";

  public static final String PORT_CONFIG = "port";
  public static final String PORT_DOC = "Port number that will be used by some service.";

  // configs of produce service
  public static final String PRODUCER_CLASS_CONFIG = "produce.producer.class";
  public static final String PRODUCER_CLASS_DOC = "Producer class that will be instantiated as producer in the produce service. "
                                                  + "It can be NewProducer, or full class name of any class that implements the KMBaseProducer interface. ";

  public static final String PRODUCER_ID_CONFIG = "produce.producer.id";
  public static final String PRODUCER_ID_DOC = "Client id that will be used in the record sent by produce service.";

  public static final String PRODUCE_RECORD_DELAY_MS_CONFIG = "produce.record.delay.ms";
  public static final String PRODUCE_RECORD_DELAY_MS_DOC = "The gap in ms between records sent to the same partition by produce service.";

  public static final String PRODUCE_RECORD_SIZE_BYTE_CONFIG = "produce.record.size.byte";
  public static final String PRODUCE_RECORD_SIZE_BYTE_DOC = "Size of record in bytes sent by produce service.";

  public static final String PRODUCE_THREAD_NUM_CONFIG = "produce.thread.num";
  public static final String PRODUCE_THREAD_NUM_DOC = "Number of threads that produce service uses to send records.";

  public static final String PRODUCER_PROPS_FILE_CONFIG = "produce.producer.props.file";
  public static final String PRODUCER_PROPS_FILE_DOC = "The properties specified in the file will be used to config producer used in produce service. "
                                                       + " The properties in the file will be overridden by properties specified as command line argument.";

  // configs of consume service
  public static final String CONSUME_CLASS_CONFIG = "consume.consumer.class";
  public static final String CONSUME_CLASS_DOC = "Consumer class that will be instantiated as consumer in the consume service. "
    + "It can be NewConsumer, OldConsumer, or full class name of any class that implements the KMBaseConsumer interface.";

  public static final String LATENCY_PERCENTILE_MAX_MS_CONFIG = "consume.latency.percentile.max.ms";
  public static final String LATENCY_PERCENTILE_MAX_MS_DOC = "This is used to derive the bucket number used to configure latency percentile metric. "
                                                             + "Any latency larger than this max value will be rounded down to the max value.";

  public static final String LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG = "consume.latency.percentile.granularity.ms";
  public static final String LATENCY_PERCENTILE_GRANULARITY_MS_DOC = "This is used to derive the bucket number used to configure latency percentile metric. "
                                                                     + "The latency at the specified percentile should be multiple of this value.";

  public static final String CONSUMER_PROPS_FILE_CONFIG = "consume.consumer.props.file";
  public static final String CONSUMER_PROPS_FILE_DOC = "The properties specified in the file will be used to config consumer used in consume service. "
                                                       + " The properties in the file will be overridden by properties specified as command line argument.";

  // configs of ExportMetrics service
  public static final String EXPORT_METRICS_NAMES_CONFIG = "export.metrics.names";
  public static final String EXPORT_METRICS_NAMES_DOC = "A list of JMX metrics that we will export in the ExportMetrics service. "
                                                  + "The list should be in the form <code>metricName1:attribute1,metricName1:attribute2,...<code>";

  public static final String EXPORT_METRICS_REPORT_INTERVAL_SEC_CONFIG = "export.metrics.report.interval.sec";
  public static final String EXPORT_METRICS_REPORT_INTERVAL_SEC_DOC = "The interval in second by which ExportMetrics service will export the metrics values.";

  static {
    CONFIG = new ConfigDef().define(ZOOKEEPER_CONNECT_CONFIG,
                                    ConfigDef.Type.STRING,
                                    "",
                                    ConfigDef.Importance.HIGH,
                                    ZOOKEEPER_CONNECT_DOC)
                            .define(BOOTSTRAP_SERVERS_CONFIG,
                                    ConfigDef.Type.STRING,
                                    "",
                                    ConfigDef.Importance.HIGH,
                                    BOOTSTRAP_SERVERS_DOC)
                            .define(TOPIC_CONFIG,
                                    ConfigDef.Type.STRING,
                                    "kafka-monitor-topic",
                                    ConfigDef.Importance.LOW,
                                    TOPIC_DOC)
                            .define(PORT_CONFIG,
                                    ConfigDef.Type.INT,
                                    8000,
                                    ConfigDef.Importance.LOW,
                                    PORT_DOC)
                            .define(PRODUCER_CLASS_CONFIG,
                                    ConfigDef.Type.STRING,
                                    NewProducer.class.getCanonicalName(),
                                    ConfigDef.Importance.LOW,
                                    PRODUCER_CLASS_DOC)
                            .define(PRODUCER_ID_CONFIG,
                                    ConfigDef.Type.STRING,
                                    "kmf-producer",
                                    ConfigDef.Importance.LOW,
                                    PRODUCER_ID_DOC)
                            .define(PRODUCE_RECORD_DELAY_MS_CONFIG,
                                    ConfigDef.Type.INT,
                                    100,
                                    ConfigDef.Importance.LOW,
                                    PRODUCE_RECORD_DELAY_MS_DOC)
                            .define(PRODUCE_RECORD_SIZE_BYTE_CONFIG,
                                    ConfigDef.Type.INT,
                                    100,
                                    ConfigDef.Importance.LOW,
                                    PRODUCE_RECORD_SIZE_BYTE_DOC)
                            .define(PRODUCE_THREAD_NUM_CONFIG,
                                    ConfigDef.Type.INT,
                                    5,
                                    ConfigDef.Importance.LOW,
                                    PRODUCE_THREAD_NUM_DOC)
                            .define(PRODUCER_PROPS_FILE_CONFIG,
                                    ConfigDef.Type.STRING,
                                    "",
                                    ConfigDef.Importance.LOW,
                                    PRODUCER_PROPS_FILE_DOC)
                            .define(CONSUME_CLASS_CONFIG,
                                    ConfigDef.Type.STRING,
                                    OldConsumer.class.getCanonicalName(),
                                    ConfigDef.Importance.LOW,
                                    CONSUME_CLASS_DOC)
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
                            .define(CONSUMER_PROPS_FILE_CONFIG,
                                    ConfigDef.Type.STRING,
                                    "",
                                    ConfigDef.Importance.LOW,
                                    CONSUMER_PROPS_FILE_DOC)
                            .define(EXPORT_METRICS_NAMES_CONFIG,
                                    ConfigDef.Type.LIST,
                                    "",
                                    ConfigDef.Importance.LOW,
                                    EXPORT_METRICS_NAMES_DOC)
                            .define(EXPORT_METRICS_REPORT_INTERVAL_SEC_CONFIG,
                                    ConfigDef.Type.INT,
                                    1,
                                    ConfigDef.Importance.LOW,
                                    EXPORT_METRICS_REPORT_INTERVAL_SEC_DOC);

  }

  public ServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
