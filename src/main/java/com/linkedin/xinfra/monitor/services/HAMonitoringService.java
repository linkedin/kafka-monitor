package com.linkedin.xinfra.monitor.services;

import com.linkedin.xinfra.monitor.services.configs.HAMonitoringConfig;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service that creates HAMonitoringCoordinator and regularly polls it.
 */
public class HAMonitoringService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(HAMonitoringService.class);
  private ScheduledExecutorService _executor;
  private AtomicBoolean _isRunning;
  private final String _name;
  private final HAMonitoringCoordinator _coordinator;

  public HAMonitoringService(Map<String, Object> props, String name, Runnable startMonitor, Runnable stopMonitor) {
    _name = name;
    HAMonitoringConfig config = new HAMonitoringConfig(props);

    long retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
    int requestTimeoutMs = config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG);
    int heartbeatIntervalMs = config.getInt(HAMonitoringConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
    String metricGrpPrefix = config.getString(HAMonitoringConfig.METRIC_GROUP_PREFIX_CONFIG);
    String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);

    Time time = Time.SYSTEM;

    // Each instance the monitor will need an ID to join the coordinator group
    String groupInstanceId = config.getString(HAMonitoringConfig.GROUP_INSTANCE_ID_CONFIG);
    if (groupInstanceId == null) {
      groupInstanceId = UUID.randomUUID().toString();
    }
    JoinGroupRequest.validateGroupInstanceId(groupInstanceId);
    String groupId = config.getString(HAMonitoringConfig.GROUP_ID_CONFIG);

    HAMonitoringIdentity id = new HAMonitoringIdentity(groupInstanceId);

    // Create paramaters required by the coordinator
    MetricConfig metricConfig = new MetricConfig();
    Metrics metrics = new Metrics(metricConfig, new ArrayList<>(), time);

    LogContext logContext = new LogContext("[HA Leader Election group=" + groupId + " instance=" + groupInstanceId + "] ");

    Metadata metadata = new Metadata(
            retryBackoffMs,
            config.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG),
            logContext,
            new ClusterResourceListeners()
    );

    final List<InetSocketAddress> addresses = ClientUtils
            .parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                    config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG));
    metadata.bootstrap(addresses, time.milliseconds());

    ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time);

    NetworkClient netClient = new NetworkClient(
            new Selector(config.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG), // maxIdleMs,
                    metrics,
                    time,
                    metricGrpPrefix,
                    channelBuilder,
                    logContext),
            metadata,
            clientId,
            100,
            config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
            config.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
            config.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
            config.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
            requestTimeoutMs,
            ClientDnsLookup.forConfig(config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG)),
            time,
            true,
            new ApiVersions(),
            logContext
    );

    ConsumerNetworkClient client = new ConsumerNetworkClient(
            logContext,
            netClient,
            metadata,
            time,
            retryBackoffMs,
            requestTimeoutMs,
            heartbeatIntervalMs
    );

    GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
            config.getInt(HAMonitoringConfig.SESSION_TIMEOUT_MS_CONFIG),
            config.getInt(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG),
            heartbeatIntervalMs,
            groupId,
            Optional.of(groupInstanceId),
            retryBackoffMs,
            false
    );

    // Create a HAMonitoringCoordinator instance
    _coordinator = new HAMonitoringCoordinator(
            groupRebalanceConfig,
            logContext,
            client,
            metrics,
            metricGrpPrefix,
            time,
            startMonitor,
            stopMonitor,
            id
    );

    _isRunning = new AtomicBoolean(true);

    /**
     * Start a thread to poll the coordinator at a fixed rate.
     */
    long initialDelaySecond = 5;
    long periodSecond = 30;

    _coordinator.ensureActiveGroup();

    _executor = Executors.newSingleThreadScheduledExecutor();
    _executor.scheduleAtFixedRate(() -> _coordinator.poll(), initialDelaySecond, periodSecond, TimeUnit.SECONDS);

    LOG.info("{}/HAMonitoringService started.", _name);
  }

  @Override
  public synchronized void start() {
    if (!_isRunning.compareAndSet(false, true)) {
      return;
    }

    // start a thread to ensure coordinator ready
    // while not stopping, poll the coordinator
//    long initialDelaySecond = 5;
//    long periodSecond = 30;
//
//    _coordinator.ensureActiveGroup();
//
//    _executor = Executors.newSingleThreadScheduledExecutor();
//    _executor.scheduleAtFixedRate(() -> _coordinator.poll(), initialDelaySecond, periodSecond, TimeUnit.SECONDS);
  }

  @Override
  public synchronized void stop() {
    /**
     * This service should contine running to see if this group member becomes in charge of reporting metrics.
     * Continue polling the cooridnator.
     */
  }

  @Override
  public boolean isRunning() {
    return _isRunning.get();
  }

  @Override
  public void awaitShutdown(long timeout, TimeUnit unit) {
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted when waiting for {}/HAMonitoringService to shutdown.", _name);
    }
    LOG.info("{}/HAMonitoringService shutdown completed.", _name);
  }
}
