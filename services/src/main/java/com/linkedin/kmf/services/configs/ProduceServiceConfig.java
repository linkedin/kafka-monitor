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

import com.linkedin.kmf.producer.NewProducer;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import java.util.Map;

public class ProduceServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String ZOOKEEPER_CONNECT_CONFIG = CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG;
  public static final String ZOOKEEPER_CONNECT_DOC = CommonServiceConfig.ZOOKEEPER_CONNECT_DOC;

  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonServiceConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = CommonServiceConfig.BOOTSTRAP_SERVERS_DOC;

  public static final String TOPIC_CONFIG = CommonServiceConfig.TOPIC_CONFIG;
  public static final String TOPIC_DOC = CommonServiceConfig.TOPIC_DOC;

  public static final String PRODUCER_CLASS_CONFIG = "produce.producer.class";
  public static final String PRODUCER_CLASS_DOC = "Producer class that will be instantiated as producer in the produce service. "
                                                  + "It can be NewProducer, or full class name of any class that implements the KMBaseProducer interface. ";

  public static final String PRODUCE_SYNC_CONFIG = "produce.sync";
  public static final String PRODUCE_SYNC_DOC = "If true, and if this is supported by the producer class, messages are sent synchronously";

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
                                    ConfigDef.Importance.MEDIUM,
                                    TOPIC_DOC)
                            .define(PRODUCER_CLASS_CONFIG,
                                    ConfigDef.Type.STRING,
                                    NewProducer.class.getCanonicalName(),
                                    ConfigDef.Importance.LOW,
                                    PRODUCER_CLASS_DOC)
                            .define(PRODUCE_SYNC_CONFIG,
                                    ConfigDef.Type.BOOLEAN,
                                    true,
                                    ConfigDef.Importance.LOW,
                                    PRODUCE_SYNC_DOC)
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
                                    PRODUCER_PROPS_FILE_DOC);

  }

  public ProduceServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
