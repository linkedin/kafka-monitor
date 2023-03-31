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

import com.linkedin.xinfra.monitor.partitioner.NewKMPartitioner;
import com.linkedin.xinfra.monitor.producer.NewProducer;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class ProduceServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonServiceConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = CommonServiceConfig.BOOTSTRAP_SERVERS_DOC;

  public static final String TOPIC_CONFIG = CommonServiceConfig.TOPIC_CONFIG;
  public static final String TOPIC_DOC = CommonServiceConfig.TOPIC_DOC;

  public static final String PRODUCER_CLASS_CONFIG = "produce.producer.class";
  public static final String PRODUCER_CLASS_DOC = "Producer class that will be instantiated as producer in the produce service. "
                                                  + "It can be NewProducer, or full class name of any class that implements the KMBaseProducer interface. ";

  public static final String PARTITIONER_CLASS_CONFIG = "produce.partitioner.class";
  public static final String PARTITIONER_CLASS_DOC = "KMPartitioner class that corresponds to the partitioner used in the target cluster.";

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

  public static final String PRODUCER_PROPS_CONFIG = "produce.producer.props";
  public static final String PRODUCER_PROPS_DOC = "The properties used to config producer in produce service.";

  public static final String LATENCY_PERCENTILE_MAX_MS_CONFIG = "produce.latency.percentile.max.ms";
  public static final String LATENCY_PERCENTILE_MAX_MS_DOC = "This is used to derive the bucket number used to configure latency percentile metric. "
                                                             + "Any latency larger than this max value will be rounded down to the max value.";

  public static final String LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG = "produce.latency.percentile.granularity.ms";
  public static final String LATENCY_PERCENTILE_GRANULARITY_MS_DOC = "This is used to derive the bucket number used to configure latency percentile metric. "
                                                                     + "The latency at the specified percentile should be multiple of this value.";

  public static final String PRODUCER_TREAT_ZERO_THROUGHPUT_AS_UNAVAILABLE_CONFIG = "produce.treat.zero.throughput.as.unavailable";
  public static final String PRODUCER_TREAT_ZERO_THROUGHPUT_AS_UNAVAILABLE_DOC = "If it is set to true, produce availability is set to 0 " +
      "if no message can be produced, regardless of whether there is exception. If this is set to false, availability will only drop below 1 if there is exception " +
      "thrown from producer. Depending on the producer configuration, it may take a few minutes for producer to be blocked before it throws exception. Advanced user " +
      "may want to set this flag to false to exactly measure the availability experienced by users";

  static {
    CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG,
                                    ConfigDef.Type.STRING,
                                    ConfigDef.Importance.HIGH,
                                    BOOTSTRAP_SERVERS_DOC)
                            .define(TOPIC_CONFIG,
                                    ConfigDef.Type.STRING,
                                    ConfigDef.Importance.HIGH,
                                    TOPIC_DOC)
                            .define(PRODUCER_CLASS_CONFIG,
                                    ConfigDef.Type.STRING,
                                    NewProducer.class.getCanonicalName(),
                                    ConfigDef.Importance.LOW,
                                    PRODUCER_CLASS_DOC)
                            .define(PARTITIONER_CLASS_CONFIG,
                                    ConfigDef.Type.CLASS,
                                    NewKMPartitioner.class,
                                    ConfigDef.Importance.HIGH,
                                    PARTITIONER_CLASS_DOC)
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
                            .define(PRODUCER_TREAT_ZERO_THROUGHPUT_AS_UNAVAILABLE_CONFIG,
                                    ConfigDef.Type.BOOLEAN,
                                    true,
                                    ConfigDef.Importance.MEDIUM,
                                    PRODUCER_TREAT_ZERO_THROUGHPUT_AS_UNAVAILABLE_DOC)
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
                            .define(PRODUCE_THREAD_NUM_CONFIG,
                                    ConfigDef.Type.INT,
                                    5,
                                    atLeast(1),
                                    ConfigDef.Importance.LOW,
                                    PRODUCE_THREAD_NUM_DOC);
  }

  public ProduceServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
