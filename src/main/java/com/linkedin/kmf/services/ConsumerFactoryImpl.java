/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services;

import com.linkedin.kmf.consumer.KMBaseConsumer;
import com.linkedin.kmf.consumer.NewConsumer;
import com.linkedin.kmf.services.configs.CommonServiceConfig;
import com.linkedin.kmf.services.configs.ConsumeServiceConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerFactoryImpl implements ConsumerFactory {
  private final KMBaseConsumer _baseConsumer;
  private String _topic;
  private static final String FALSE = "false";
  private final int _latencyPercentileMaxMs;
  private final int _latencyPercentileGranularityMs;
  private static final String[] NON_OVERRIDABLE_PROPERTIES =
      new String[] {ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG};
  private int _latencySlaMs;
  private static AdminClient adminClient;
  private static final Logger LOG = LoggerFactory.getLogger(ConsumerFactoryImpl.class);

  public ConsumerFactoryImpl(Map<String, Object> props) throws Exception {
    LOG.info("Creating AdminClient.");
    adminClient = AdminClient.create(props);
    Map consumerPropsOverride = props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)
        ? (Map) props.get(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG) : new HashMap<>();
    ConsumeServiceConfig config = new ConsumeServiceConfig(props);
    _topic = config.getString(ConsumeServiceConfig.TOPIC_CONFIG);
    String zkConnect = config.getString(ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    String brokerList = config.getString(ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    String consumerClassName = config.getString(ConsumeServiceConfig.CONSUMER_CLASS_CONFIG);
    _latencySlaMs = config.getInt(ConsumeServiceConfig.LATENCY_SLA_MS_CONFIG);
    _latencyPercentileMaxMs = config.getInt(ConsumeServiceConfig.LATENCY_PERCENTILE_MAX_MS_CONFIG);
    _latencyPercentileGranularityMs = config.getInt(ConsumeServiceConfig.LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG);
    for (String property: NON_OVERRIDABLE_PROPERTIES) {
      if (consumerPropsOverride.containsKey(property)) {
        throw new ConfigException("Override must not contain " + property + " config.");
      }
    }
    Properties consumerProps = new Properties();

    /* Assign default config. This has the lowest priority. */
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "kmf-consumer");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kmf-consumer-group-" + new Random().nextInt());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    if (consumerClassName.equals(NewConsumer.class.getCanonicalName()) || consumerClassName.equals(NewConsumer.class.getSimpleName())) {
      consumerClassName = NewConsumer.class.getCanonicalName();
    }

    /* Assign config specified for ConsumeService. */
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    consumerProps.put(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect);

    /* Assign config specified for consumer. This has the highest priority. */
    consumerProps.putAll(consumerPropsOverride);

    if (props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)) {
      props.forEach(consumerProps::putIfAbsent);
    }

    _baseConsumer = (KMBaseConsumer) Class.forName(consumerClassName).getConstructor(String.class, Properties.class).newInstance(_topic, consumerProps);

  }

  @Override
  public AdminClient adminClient() {
    return adminClient;
  }

  @Override
  public int latencySlaMs() {
    return _latencySlaMs;
  }

  @Override
  public KMBaseConsumer baseConsumer() {
    return _baseConsumer;
  }

  @Override
  public String topic() {
    return _topic;
  }

  @Override
  public int latencyPercentileMaxMs() {
    return _latencyPercentileMaxMs;
  }

  @Override
  public int latencyPercentileGranularityMs() {
    return _latencyPercentileGranularityMs;
  }

}
