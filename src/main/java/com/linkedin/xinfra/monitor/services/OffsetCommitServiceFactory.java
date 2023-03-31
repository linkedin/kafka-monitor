/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.xinfra.monitor.XinfraMonitorConstants;
import com.linkedin.xinfra.monitor.common.Utils;
import com.linkedin.xinfra.monitor.services.configs.CommonServiceConfig;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory for OffsetCommitService
 */
@SuppressWarnings("rawtypes")
public class OffsetCommitServiceFactory implements ServiceFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(OffsetCommitServiceFactory.class);
  private final Map _properties;
  private final String _serviceName;

  public OffsetCommitServiceFactory(Map properties, String serviceName) {

    _properties = properties;
    _serviceName = serviceName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Service createService() throws JsonProcessingException {
    LOGGER.info("Creating OffsetCommitService...");
    AdminClient adminClient = AdminClient.create(_properties);

    Properties preparedProps = this.prepareConfigs(_properties);
    ConsumerConfig consumerConfig = new ConsumerConfig(preparedProps);
    LOGGER.info("OffsetCommitServiceFactory consumer config {}", Utils.prettyPrint(consumerConfig.values()));

    return new OffsetCommitService(consumerConfig, _serviceName, adminClient);
  }

  /**
   * populate configs for kafka client
   * @param props Map of String to Object
   * @return Properties
   */
  @SuppressWarnings("unchecked")
  private Properties prepareConfigs(Map<String, Object> props) {

    String brokerList = (String) props.get(CommonServiceConfig.BOOTSTRAP_SERVERS_CONFIG);

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, XinfraMonitorConstants.FALSE);
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, XinfraMonitorConstants.XINFRA_MONITOR_PREFIX + _serviceName);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

    Map<String, String> customProps = (Map<String, String>) props.get(CommonServiceConfig.CONSUMER_PROPS_CONFIG);
    if (customProps != null) {
      for (Map.Entry<String, String> entry : customProps.entrySet()) {
        consumerProps.put(entry.getKey(), entry.getValue());
      }
    }

    return consumerProps;
  }
}
