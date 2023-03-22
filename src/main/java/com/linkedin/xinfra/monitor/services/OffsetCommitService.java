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
import com.linkedin.xinfra.monitor.services.metrics.OffsetCommitServiceMetrics;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Service that monitors the commit offset availability of a particular Consumer Group.
 */
public class OffsetCommitService implements Service {

  public static final String METRIC_GRP_PREFIX = "xm-offset-commit-service";
  private static final int MAX_INFLIGHT_REQUESTS_PER_CONNECTION = 100;
  private static final Logger LOGGER = LoggerFactory.getLogger(OffsetCommitService.class);
  private static final String SERVICE_SUFFIX = "-consumer-offset-commit-service";
  private final AtomicBoolean _isRunning;
  private final ScheduledExecutorService _scheduledExecutorService;
  private final String _serviceName;
  private final AdminClient _adminClient;
  private final String _consumerGroup;

  // the consumer network client that communicates with kafka cluster brokers.
  private final ConsumerNetworkClient _consumerNetworkClient;
  private final Time _time;
  private final OffsetCommitServiceMetrics _offsetCommitServiceMetrics;

  /**
   *
   * @param config The consumer configuration keys
   * @param serviceName name of the xinfra monitor service
   * @param adminClient Administrative client for Kafka, which supports managing and inspecting topics, brokers, configurations and ACLs.
   */
  OffsetCommitService(ConsumerConfig config, String serviceName, AdminClient adminClient)
      throws JsonProcessingException {

    _time = Time.SYSTEM;
    _consumerGroup = config.getString(ConsumerConfig.GROUP_ID_CONFIG);
    _adminClient = adminClient;
    _isRunning = new AtomicBoolean(false);
    _serviceName = serviceName;

    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(Service.JMX_PREFIX));
    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    Metrics metrics = new Metrics(metricConfig, reporters, _time);
    Map<String, String> tags = new HashMap<>();
    tags.put(XinfraMonitorConstants.TAGS_NAME, serviceName);

    _offsetCommitServiceMetrics = new OffsetCommitServiceMetrics(metrics, tags);

    long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
    int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);

    String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);

    List<String> bootstrapServers = config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    List<InetSocketAddress> addresses =
        ClientUtils.parseAndValidateAddresses(bootstrapServers, ClientDnsLookup.DEFAULT);

    LogContext logContext = new LogContext("[Consumer clientId=" + clientId + "] ");

    ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, _time, logContext);

    LOGGER.info("Bootstrap servers config: {} | broker addresses: {}", bootstrapServers, addresses);

    Metadata metadata = new Metadata(retryBackoffMs, config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG), logContext,
        new ClusterResourceListeners());

    metadata.bootstrap(addresses);

    Selector selector =
        new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), new Metrics(), _time,
            METRIC_GRP_PREFIX, channelBuilder, logContext);

    KafkaClient kafkaClient = new NetworkClient(
        selector, metadata, clientId, MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
        config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
        config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
        config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG), config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
        config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
        config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG), config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
        ClientDnsLookup.DEFAULT, _time, true,
        new ApiVersions(), logContext);


    LOGGER.debug("The network client active: {}", kafkaClient.active());
    LOGGER.debug("The network client has in flight requests: {}", kafkaClient.hasInFlightRequests());
    LOGGER.debug("The network client in flight request count: {}", kafkaClient.inFlightRequestCount());

    _consumerNetworkClient = new ConsumerNetworkClient(logContext, kafkaClient, metadata, _time, retryBackoffMs,
        config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), heartbeatIntervalMs);

    ThreadFactory threadFactory = new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        return new Thread(runnable, serviceName + SERVICE_SUFFIX);
      }
    };
    _scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);

    LOGGER.info("OffsetCommitService's ConsumerConfig - {}", Utils.prettyPrint(config.values()));
  }

  /**
   * The start logic must only execute once.  If an error occurs then the implementer of this class must assume that
   * stop() will be called to clean up.  This method must be thread safe and must assume that stop() may be called
   * concurrently. This can happen if the monitoring application's life cycle is being managed by a container.  Start
   * will only be called once.
   */
  @Override
  public void start() {
    if (_isRunning.compareAndSet(false, true)) {

      Runnable runnable = new OffsetCommitServiceRunnable();
      _scheduledExecutorService.scheduleWithFixedDelay(runnable, 1, 2, TimeUnit.SECONDS);
      LOGGER.info("Scheduled the offset commit service executor.");
    }
  }

  private class OffsetCommitServiceRunnable implements Runnable {
    @Override
    public void run() {
      try {
        sendOffsetCommitRequest(_consumerNetworkClient, _adminClient, _consumerGroup);
      } catch (ExecutionException | InterruptedException e) {
        LOGGER.error("OffsetCommitServiceRunnable class encountered an exception: ", e);
      }
    }
  }

  /**
   *
   * @param consumerNetworkClient Kafka consumer network client. Higher level consumer access
   *                              to the network layer with basic support for request futures.
   * @param adminClient admin client object
   * @param consumerGroup consumer group name
   * @throws ExecutionException when attempting to retrieve the result of a task that aborted by throwing an exception
   * @throws InterruptedException Thrown when the thread is waiting, sleeping, or otherwise occupied,
   *                              and the thread is interrupted, either before or during the activity.
   */
  private void sendOffsetCommitRequest(ConsumerNetworkClient consumerNetworkClient, AdminClient adminClient,
      String consumerGroup) throws ExecutionException, InterruptedException, RuntimeException {


    LOGGER.trace("Consumer groups available: {}", adminClient.listConsumerGroups().all().get());

    Node groupCoordinator = adminClient.describeConsumerGroups(Collections.singleton(consumerGroup))
        .all()
        .get()
        .get(consumerGroup)
        .coordinator();
    LOGGER.trace("Consumer group {} coordinator {}, consumer group {}", consumerGroup, groupCoordinator, consumerGroup);

    consumerNetworkClient.tryConnect(groupCoordinator);
    consumerNetworkClient.maybeTriggerWakeup();

    OffsetCommitRequestData offsetCommitRequestData = new OffsetCommitRequestData();
    AbstractRequest.Builder<?> offsetCommitRequestBuilder = new OffsetCommitRequest.Builder(offsetCommitRequestData);

    LOGGER.debug("pending request count: {}", consumerNetworkClient.pendingRequestCount());

    RequestFuture<ClientResponse> future = consumerNetworkClient.send(groupCoordinator, offsetCommitRequestBuilder);

    if (consumerNetworkClient.isUnavailable(groupCoordinator)) {
      _offsetCommitServiceMetrics.recordUnavailable();
      throw new RuntimeException("Unavailable consumerNetworkClient for " + groupCoordinator);
    } else {
      LOGGER.trace("The consumerNetworkClient is available for {}", groupCoordinator);
      if (consumerNetworkClient.hasPendingRequests()) {

        boolean consumerNetworkClientPollResult =
            consumerNetworkClient.poll(future, _time.timer(Duration.ofSeconds(5).toMillis()));
        LOGGER.debug("result of poll {}", consumerNetworkClientPollResult);

        if (future.failed() && !future.isRetriable()) {
          _offsetCommitServiceMetrics.recordFailed();
          throw future.exception();
        }

        if (future.succeeded() && future.isDone() && consumerNetworkClientPollResult) {

          ClientResponse clientResponse = future.value();

          _offsetCommitServiceMetrics.recordSuccessful();
          LOGGER.info("ClientResponseRequestFuture value {} for coordinator {} and consumer group {}", clientResponse,
              groupCoordinator, consumerGroup);
        }
      }
    }
  }

  /**
   * This may be called multiple times.  This method must be thread safe and must assume that start() may be called
   * concurrently.  This can happen if the monitoring application's life cycle is being managed by a container.
   * Implementations must be non-blocking and should release the resources acquired by the service during start().
   */
  @Override
  public void stop() {
    if (_isRunning.compareAndSet(true, false)) {
      _scheduledExecutorService.shutdown();
    }
  }

  /**
   * Implementations of this method must be thread safe as it can be called at any time.  Implementations must be
   * non-blocking.
   * @return true if this start() has returned successfully else this must return false.  This must also return false if
   * the service can no longer perform its function.
   */
  @Override
  public boolean isRunning() {
    return _isRunning.get() && !_scheduledExecutorService.isShutdown();
  }

  /**
   * Implementations of this method must be thread safe and must be blocking.
   */
  @Override
  public void awaitShutdown(long timeout, TimeUnit unit) {
    try {
      _scheduledExecutorService.awaitTermination(timeout, unit);
    } catch (InterruptedException interruptedException) {
      LOGGER.error("Thread interrupted when waiting for {} to shutdown.", _serviceName, interruptedException);
    }
    LOGGER.info("{} shutdown completed.", _serviceName);
  }
}
