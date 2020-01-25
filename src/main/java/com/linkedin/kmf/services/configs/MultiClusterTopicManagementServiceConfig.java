/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services.configs;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class MultiClusterTopicManagementServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String TOPIC_CONFIG = CommonServiceConfig.TOPIC_CONFIG;
  public static final String TOPIC_DOC = CommonServiceConfig.TOPIC_DOC;

  public static final String PROPS_PER_CLUSTER_CONFIG = "topic.management.props.per.cluster";
  public static final String PROPS_PER_CLUSTER_DOC = "A map from cluster name to a TopicManagementService config for each monitored cluster";

  public static final String REBALANCE_INTERVAL_MS_CONFIG = "topic-management.rebalance.interval.ms";
  public static final String REBALANCE_INTERVAL_MS_DOC = "The gap in ms between the times the cluster balance on the "
      + "monitor topic is checked.  Set this to a large value to disable automatic topic rebalance.";

  public static final String PREFERRED_LEADER_ELECTION_CHECK_INTERVAL_MS_CONFIG = "topic-management.preferred.leader.election.check.interval.ms";
  public static final String PREFERRED_LEADER_ELECTION_CHECK_INTERVAL_MS_DOC = "The gap in ms between the times to check if preferred leader election"
      + " can be performed when requested during rebalance";

  static {
    CONFIG = new ConfigDef()
      .define(TOPIC_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              TOPIC_DOC)
      .define(REBALANCE_INTERVAL_MS_CONFIG,
              ConfigDef.Type.INT,
              1000 * 60 * 10,
              atLeast(10),
              ConfigDef.Importance.LOW,
              REBALANCE_INTERVAL_MS_DOC)
        .define(PREFERRED_LEADER_ELECTION_CHECK_INTERVAL_MS_CONFIG,
            ConfigDef.Type.LONG,
            1000 * 60 * 5,
            atLeast(5),
            ConfigDef.Importance.LOW, PREFERRED_LEADER_ELECTION_CHECK_INTERVAL_MS_DOC);
  }

  public MultiClusterTopicManagementServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }
}
