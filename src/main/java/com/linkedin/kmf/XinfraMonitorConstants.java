/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf;

/**
 * Constant variables in Xinfra Monitor repo.
 */
public class XinfraMonitorConstants {

  public XinfraMonitorConstants() {
  }

  public static final String TOPIC_MANIPULATION_SERVICE_TOPIC =
      "xinfra-monitor-cluster-topic-manipulation-service-topic-";

  public static final String KAFKA_LOG_DIRECTORY = "/tmp/kafka-logs";

  public static final int TOPIC_MANIPULATION_TOPIC_NUM_PARTITIONS = 10;

  static final String FACTORY = "Factory";

  static final String CLASS_NAME_CONFIG = "class.name";

  static final String METRIC_GROUP_NAME = "kafka-monitor";

  static final String JMX_PREFIX = "kmf";
}
