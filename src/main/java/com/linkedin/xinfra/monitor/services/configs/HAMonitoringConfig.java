package com.linkedin.xinfra.monitor.services.configs;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigDef;

public class HAMonitoringConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;

  public static final String GROUP_ID_CONFIG = "group.id";
  public static final String GROUP_ID_DOC = "A unique string that identifies the group this member belongs to.";

  public static final String GROUP_INSTANCE_ID_CONFIG = "group.instance.id";
  public static final String GROUP_INSTANCE_ID_DOC = "A unique identifier for the application instance.";

  public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";
  public static final String HEARTBEAT_INTERVAL_MS_DOC = "The expected time between heartbeats to the coordinator " +
          "Heartbeats are used to ensure that the member's session stays active and to facilitate rebalancing when " +
          " new consumers join or leave the group. " +
          "The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher " +
          "than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.";

  public static final String METRIC_GROUP_PREFIX_CONFIG = "metric.group.prefix";
  public static final String METRIC_GROUP_PREFIX_DOC = "Prefix for the group of metrics reported.";

  public static final String MAX_POLL_INTERVAL_MS_CONFIG = "max.poll.interval.ms";
  private static final String MAX_POLL_INTERVAL_MS_DOC = "The maximum delay between invocations of poll().  " +
          "This places an upper bound on the amount of time that the member can be idle before checking for " +
          "leadership. If poll() is not called before expiration of this timeout, then the member is considered " +
          "failed and the group will rebalance in order to reassign the partitions to another member. ";

  public static final String SESSION_TIMEOUT_MS_CONFIG = "session.timeout.ms";
  private static final String SESSION_TIMEOUT_MS_DOC = "The timeout used to detect failures when using " +
          "The member sends periodic heartbeats to indicate its liveness to the broker. " +
          "If no heartbeats are received by the broker before the expiration of this session timeout, " +
          "then the broker will remove this member from the group and initiate a rebalance. Note that the value " +
          "must be in the allowable range as configured in the broker configuration by " +
          " <code>group.min.session.timeout.ms</code> and <code>group.max.session.timeout.ms</code>.";

  static {
    CONFIG = new ConfigDef().define(GROUP_ID_CONFIG,
                                    ConfigDef.Type.STRING,
                                    "xinfra-monitor-leader",
                                    ConfigDef.Importance.HIGH,
                                    GROUP_ID_DOC)
                            .define(GROUP_INSTANCE_ID_CONFIG,
                                    ConfigDef.Type.STRING,
                                    null,
                                    ConfigDef.Importance.MEDIUM,
                                    GROUP_INSTANCE_ID_DOC)
                            .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                    ConfigDef.Type.STRING,
                                    CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                                    ConfigDef.Importance.MEDIUM,
                                    CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                            .define(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
                                    ConfigDef.Type.LONG,
                                    100L,
                                    ConfigDef.Range.atLeast(0L),
                                    ConfigDef.Importance.LOW,
                                    CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                            .define(CommonClientConfigs.METADATA_MAX_AGE_CONFIG,
                                    ConfigDef.Type.LONG,
                                    TimeUnit.MINUTES.toMillis(5),
                                    ConfigDef.Range.atLeast(0),
                                    ConfigDef.Importance.LOW,
                                    CommonClientConfigs.METADATA_MAX_AGE_DOC)
                            .define(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                                    ConfigDef.Type.LONG,
                                    50L,
                                    ConfigDef.Range.atLeast(0L),
                                    ConfigDef.Importance.LOW,
                                    CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                            .define(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                                    ConfigDef.Type.LONG,
                                    TimeUnit.SECONDS.toMillis(1),
                                    ConfigDef.Range.atLeast(0L),
                                    ConfigDef.Importance.LOW,
                                    CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                            .define(CommonClientConfigs.SEND_BUFFER_CONFIG,
                                    ConfigDef.Type.INT,
                                    128 * 1024,
                                    ConfigDef.Range.atLeast(0),
                                    ConfigDef.Importance.MEDIUM,
                                    CommonClientConfigs.SEND_BUFFER_DOC)
                            .define(CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                                    ConfigDef.Type.INT,
                                    32 * 1024,
                                    ConfigDef.Range.atLeast(0),
                                    ConfigDef.Importance.MEDIUM,
                                    CommonClientConfigs.RECEIVE_BUFFER_DOC)
                            .define(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
                                    ConfigDef.Type.INT,
                                    Math.toIntExact(TimeUnit.SECONDS.toMillis(40)),
                                    ConfigDef.Range.atLeast(0),
                                    ConfigDef.Importance.MEDIUM,
                                    CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
                            .define(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
                                    ConfigDef.Type.STRING,
                                    ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                    ConfigDef.ValidString.in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                            ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                                    ConfigDef.Importance.MEDIUM,
                                    CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                            .define(HEARTBEAT_INTERVAL_MS_CONFIG,
                                    ConfigDef.Type.INT,
                                    Math.toIntExact(TimeUnit.SECONDS.toMillis(3)),
                                    ConfigDef.Importance.HIGH,
                                    HEARTBEAT_INTERVAL_MS_DOC)
                            .define(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                                    ConfigDef.Type.LONG,
                                    9 * 60 * 1000,
                                    ConfigDef.Importance.MEDIUM,
                                    CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                            .define(METRIC_GROUP_PREFIX_CONFIG,
                                    ConfigDef.Type.STRING,
                                    "xinfra-leader-election",
                                    ConfigDef.Importance.LOW,
                                    METRIC_GROUP_PREFIX_DOC)
                            .define(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                                    ConfigDef.Type.LIST,
                                    Collections.emptyList(),
                                    new ConfigDef.NonNullValidator(),
                                    ConfigDef.Importance.HIGH,
                                    CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                            .define(MAX_POLL_INTERVAL_MS_CONFIG,
                                    ConfigDef.Type.INT,
                                    60000,
                                    ConfigDef.Range.atLeast(1),
                                    ConfigDef.Importance.MEDIUM,
                                    MAX_POLL_INTERVAL_MS_DOC)
                            .define(SESSION_TIMEOUT_MS_CONFIG,
                                    ConfigDef.Type.INT,
                                    10000,
                                    ConfigDef.Importance.HIGH,
                                    SESSION_TIMEOUT_MS_DOC)
                            .define(CommonClientConfigs.CLIENT_ID_CONFIG,
                                    ConfigDef.Type.STRING,
                                    "",
                                    ConfigDef.Importance.LOW,
                                    CommonClientConfigs.CLIENT_ID_DOC)
                            .define(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG,
                                    ConfigDef.Type.INT,
                                    Math.toIntExact(TimeUnit.MINUTES.toMillis(1)),
                                    ConfigDef.Importance.HIGH,
                                    CommonClientConfigs.REBALANCE_TIMEOUT_MS_DOC)
                            .withClientSaslSupport();
  }

  public HAMonitoringConfig(Map<?, ?> props) { 
    super(CONFIG, props); 
  }
}
