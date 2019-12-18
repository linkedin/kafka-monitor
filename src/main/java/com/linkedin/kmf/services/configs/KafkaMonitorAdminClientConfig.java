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

import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;


public class KafkaMonitorAdminClientConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = CommonClientConfigs.BOOTSTRAP_SERVERS_DOC;

  public static final String TOPIC_CONFIG = "topic";
  public static final String TOPIC_DOC = "Topic to be used by this service.";


  static {
    CONFIG = new ConfigDef()
        .define(BOOTSTRAP_SERVERS_CONFIG,
            ConfigDef.Type.STRING,
            "andchoi-mn2.linkedin.biz:9092",
            ConfigDef.Importance.HIGH,
            BOOTSTRAP_SERVERS_DOC)
        .define(TOPIC_CONFIG,
            ConfigDef.Type.STRING,
            "messi400",
            ConfigDef.Importance.HIGH,
            TOPIC_DOC)
        .define(org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG,
            ConfigDef.Type.STRING,
            SecurityProtocol.SSL.name,
            ConfigDef.Importance.HIGH,
            org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG)
        .define(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            ConfigDef.Type.STRING,
            "changeit",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
        .define(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            ConfigDef.Type.STRING,
            "/etc/riddler/cacerts",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)
        .define(SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            ConfigDef.Type.STRING,
            "work_around_jdk-6879539",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG)
        .define(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            ConfigDef.Type.STRING,
            "work_around_jdk-6879539",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)
        .define(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
            ConfigDef.Type.STRING,
            "SunX509",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG)
        .define(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            ConfigDef.Type.STRING,
            "/export/content/lid/apps/kafka/i001/var/identity.p12",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)
        .define(SslConfigs.SSL_PROTOCOL_CONFIG,
            ConfigDef.Type.STRING,
            "TLS",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_PROTOCOL_CONFIG)
        .define(org.apache.kafka.clients.admin.AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
            ConfigDef.Type.INT,
            50000,
            atLeast(1),
            ConfigDef.Importance.HIGH,
            org.apache.kafka.clients.admin.AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG)
        .define(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            "pkcs12",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)
        .define(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
            ConfigDef.Type.STRING,
            "SHA1PRNG",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG)
        .define(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
            ConfigDef.Type.STRING,
            "SunX509",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG)
        .define(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            "JKS",
            ConfigDef.Importance.HIGH,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
  }

  public KafkaMonitorAdminClientConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }

}

