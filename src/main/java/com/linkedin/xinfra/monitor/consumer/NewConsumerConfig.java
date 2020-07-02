/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.xinfra.monitor.consumer;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;


/**
 * Configuration for Xinfra Monitor New Consumer
 */
public class NewConsumerConfig extends AbstractConfig {

  private static final ConfigDef CONFIG_DEF;

  public static final String TARGET_CONSUMER_GROUP_ID_CONFIG = "target.consumer.group.id";
  public static final String TARGET_CONSUMER_GROUP_ID_CONFIG_DOC =
      "When defined a consumer group is chosen such that it maps to the same group coordinator as the specified "
          + "group coordinator.";

  static {
    CONFIG_DEF = new ConfigDef().define(TARGET_CONSUMER_GROUP_ID_CONFIG,
                                    ConfigDef.Type.STRING,
                                    null,
                                    ConfigDef.Importance.MEDIUM,
                                    TARGET_CONSUMER_GROUP_ID_CONFIG_DOC);
  }

  public NewConsumerConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
  }
}

