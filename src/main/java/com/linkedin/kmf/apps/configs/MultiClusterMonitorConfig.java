/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.apps.configs;

import com.linkedin.kmf.services.configs.CommonServiceConfig;
import com.linkedin.kmf.services.configs.MultiClusterTopicManagementServiceConfig;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;


public class MultiClusterMonitorConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String TOPIC_CONFIG = CommonServiceConfig.TOPIC_CONFIG;
  public static final String TOPIC_DOC = CommonServiceConfig.TOPIC_DOC;

  public static final String CONSUME_SERVICE_CONFIG = "consume.service.props";
  public static final String CONSUME_SERVICE_DOC = "The properties used to config the consume service.";

  public static final String PRODUCE_SERVICE_CONFIG = "produce.service.props";
  public static final String PRODUCE_SERVICE_DOC = "The properties used to config the produce service.";

  public static final String TOPIC_MANAGEMENT_SERVICE_CONFIG = MultiClusterTopicManagementServiceConfig.PROPS_PER_CLUSTER_CONFIG;
  public static final String TOPIC_MANAGEMENT_SERVICE_DOC = MultiClusterTopicManagementServiceConfig.PROPS_PER_CLUSTER_DOC;

  public Double getDouble(String key) {
    return (Double) get(key);
  }

  static {
    CONFIG = new ConfigDef()
      .define(TOPIC_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              TOPIC_DOC);
  }

  public MultiClusterMonitorConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }

}
