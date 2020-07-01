/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Service that monitors the commit offset availability of a particular Consumer Group.
 */
@SuppressWarnings("NullableProblems")
public class OffsetCommitService implements Service {

  private static final int MAX_INFLIGHT_REQUESTS_PER_CONNECTION = 100;
  private static final Logger LOGGER = LoggerFactory.getLogger(OffsetCommitService.class);
  private static final String METRIC_GRP_PREFIX = "xm-offset-commit-service";
  private static final String SERVICE_SUFFIX = "-consumer-offset-commit-service";
  private final AtomicBoolean _isRunning;
  private final ScheduledExecutorService _scheduledExecutorService;
  //  private final ConsumerNetworkClient _consumerNetworkClient;
  private final String _serviceName;
  private final AdminClient _adminClient;
  private final String _consumerGroup;

  // the actual network client that talks to kafka brokers
  private final ConsumerNetworkClient _client;
  private final Metadata _metadata;
  private final long _retryBackoffMs;
  private final long _requestTimeoutMs;
  private final Time _time;
  private final IsolationLevel _isolationLevel;

  /**
   *
   * @param config The consumer configuration keys
   * @param serviceName name of the xinfra monitor service
   * @param adminClient Administrative client for Kafka, which supports managing and inspecting topics, brokers, configurations and ACLs.
   */
  OffsetCommitService(ConsumerConfig config, String serviceName, AdminClient adminClient) {
    this._retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
    this._requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
    String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
    LogContext logContext = new LogContext("[Consumer clientId=" + clientId + "] ");
    this._metadata = new Metadata(_retryBackoffMs, config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG), logContext,
        new ClusterResourceListeners());

    this._time = Time.SYSTEM;

    List<InetSocketAddress> addresses =
        ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
            config.getString(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG));

//    this._metadata.update(Cluster.bootstrap(addresses), Collections.emptySet(), _time.milliseconds());
    ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, _time);
    String metricGrpPrefix = "kcc";

    int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
    this._isolationLevel =
        IsolationLevel.valueOf(config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));

    Selector selector =
        new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), new Metrics(), _time,
            metricGrpPrefix, channelBuilder, logContext);

    NetworkClient netClient = new NetworkClient(selector, this._metadata, clientId, 100,
        config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
        config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
        config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG), config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
        config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), ClientDnsLookup.USE_ALL_DNS_IPS, _time, false,
        new ApiVersions(), null, new LogContext());
    this._client = new ConsumerNetworkClient(logContext, netClient, _metadata, _time, _retryBackoffMs,
        config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), heartbeatIntervalMs);

    _adminClient = adminClient;
    _isRunning = new AtomicBoolean(false);
    _serviceName = serviceName;
    _consumerGroup = config.getString(ConsumerConfig.GROUP_ID_CONFIG);
//
    ThreadFactory threadFactory = new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        return new Thread(runnable, serviceName + SERVICE_SUFFIX);
      }
    };
    _scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
//
//    Time time = Time.SYSTEM;
//    ClientDnsLookup clientDnsLookup = ClientDnsLookup.DEFAULT;
//    ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time);
//
//    int requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
//    int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
//    long refreshBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
//    String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
//
//    Metrics metrics = new Metrics();
//    LogContext logContext = new LogContext();
//    ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
//
//    Selector selector = new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time,
//        OffsetCommitService.METRIC_GRP_PREFIX, channelBuilder, logContext);
//
//    Metadata metadata =
//        new Metadata(refreshBackoffMs, config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG), logContext,
//            clusterResourceListeners);
//
////    metadata.update(MetadataResponse);
//
////    List<InetSocketAddress> addresses =
////        ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
////            config.getString(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG));
////    metadata.update(2, MetadataResponse.prepareResponse(null, "", 3, new ArrayList<>()), time.milliseconds());
//    ApiVersions apiVersions = new ApiVersions();
//
//    // Fixed, large enough value will suffice for a max in-flight requests.
//    KafkaClient kafkaClient = new NetworkClient(selector, metadata, clientId, MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
//        config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
//        config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
//        config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG), config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
//        config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), clientDnsLookup, time, true, apiVersions, logContext);
//
//    _consumerNetworkClient =
//        new ConsumerNetworkClient(logContext, kafkaClient, metadata, time, refreshBackoffMs, requestTimeoutMs,
//            heartbeatIntervalMs);
//
//    LOGGER.info("OffsetCommitService's ConsumerConfig - {}", config.values());
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
        sendOffsetCommitRequest(_client, _adminClient, _consumerGroup);
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
      String consumerGroup) throws ExecutionException, InterruptedException {

    LOGGER.info("Sending Offset Commit Request to get RequestFuture<ClientResponse>.");
    LOGGER.info("consumer groups available: {}", adminClient.listConsumerGroups().all().get());

    Node groupCoordinator = adminClient.describeConsumerGroups(Collections.singleton(consumerGroup))
        .all()
        .get()
        .get(consumerGroup)
        .coordinator();

    LOGGER.info("CG coordinator {}", groupCoordinator);

//    Readable readable = new ByteBufferAccessor(ByteBuffer.allocate(50));
//    OffsetCommitRequestData data = new OffsetCommitRequestData(readable, (short) 7);

//    OffsetCommitRequestData offsetCommitRequestData = new OffsetCommitRequestData();
//    AbstractRequest.Builder<?> offsetCommitRequestBuilder = new OffsetCommitRequest.Builder(offsetCommitRequestData);
//    RequestFuture<ClientResponse> clientResponseRequestFuture =
//        consumerNetworkClient.send(coordinator, offsetCommitRequestBuilder);

//    Node node = consumerNetworkClient.leastLoadedNode();
//    LOGGER.info("consumer network client least loaded node, {}", node);
//    if (node == null)
//      return RequestFuture.noBrokersAvailable();
//    else
    RequestFuture<ClientResponse> clientResponseRequestFuture =
        consumerNetworkClient.send(groupCoordinator, MetadataRequest.Builder.allTopics());
//    LOGGER.info("{}", consumerNetworkClient.leastLoadedNode());
    LOGGER.info("{}", consumerNetworkClient.isUnavailable(groupCoordinator));
    LOGGER.info("{}", consumerNetworkClient.hasReadyNodes(2));
    LOGGER.info("{}", consumerNetworkClient.hasPendingRequests());
    LOGGER.info("{}", consumerNetworkClient.pendingRequestCount());
    consumerNetworkClient.maybeTriggerWakeup();

//    _client.poll(clientResponseRequestFuture, _time.timer(_requestTimeoutMs));
    LOGGER.info("value: {}", clientResponseRequestFuture.value()); // blocking

    if (clientResponseRequestFuture.succeeded()) {
      LOGGER.info("value: {}", clientResponseRequestFuture.value()); // blocking
    }

    if (clientResponseRequestFuture.isDone()) {
      LOGGER.info("ClientResponseRequestFuture value {} ", clientResponseRequestFuture.value());
      ClientResponse clientResponse = clientResponseRequestFuture.value();

      clientResponse.onComplete();
      LOGGER.info("RequestFuture<ClientResponse> is complete.");
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
