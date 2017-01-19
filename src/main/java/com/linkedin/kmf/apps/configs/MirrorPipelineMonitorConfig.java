/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.apps.configs;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;


public class MirrorPipelineMonitorConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String CONSUME_SERVICE_CONFIG = "consume.props";
  public static final String CONSUME_SERVICE_DOC = "The properties used to config the consume service.";

  public static final String PRODUCE_SERVICE_CONFIG = "produce.props";
  public static final String PRODUCE_SERVICE_DOC = "The properties used to config the produce service.";

  public static final String TOPIC_MANAGEMENT_SERVICE_CONFIG = "topicManagement.props";
  public static final String TOPIC_MANAGEMENT_SERVICE_DOC = "The properties used to config the topic management service.";

  public static final String ZOOKEEPER_CONNECT_LIST_CONFIG = "zookeeper.connect.list";
  public static final String ZOOKEEPER_CONNECT_LIST_DOC = "List of zookeeper connection strings";

  public Double getDouble(String key) {
    return (Double) get(key);
  }

  static {
    CONFIG = new ConfigDef().define(ZOOKEEPER_CONNECT_LIST_CONFIG,
                                    ConfigDef.Type.LIST,
                                    ConfigDef.Importance.HIGH,
                                    ZOOKEEPER_CONNECT_LIST_DOC);
  }

  public MirrorPipelineMonitorConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
