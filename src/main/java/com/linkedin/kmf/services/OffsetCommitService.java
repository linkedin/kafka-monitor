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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.OffsetCommitRequest;
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
  private final AtomicBoolean _isRunning;
  private final ScheduledExecutorService _scheduledExecutorService;
  private final ConsumerNetworkClient _consumerNetworkClient;
  private final String _serviceName;

  OffsetCommitService(ConsumerConfig config, String serviceName) {

    _isRunning = new AtomicBoolean(false);
    _serviceName = serviceName;
    ThreadFactory threadFactory = new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        return new Thread(runnable, serviceName + "-consumer-offset-commit-service");
      }
    };
    _scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);

    Time time = Time.SYSTEM;
    ClientDnsLookup clientDnsLookup = ClientDnsLookup.DEFAULT;
    ChannelBuilder sslChannelBuilder = ClientUtils.createChannelBuilder(config, time);

    int requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
    int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
    Long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
    String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);

    Metrics metrics = new Metrics();
    LogContext context = new LogContext();
    ClusterResourceListeners listeners = new ClusterResourceListeners();

    Selector selector =
        new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, METRIC_GRP_PREFIX,
            sslChannelBuilder, context);

    Metadata metadata = new Metadata(retryBackoffMs, requestTimeoutMs, context, listeners);
    ApiVersions apiVersions = new ApiVersions();

    // Fixed, large enough value will suffice for a max in-flight requests.
    KafkaClient kafkaClient = new NetworkClient(selector, metadata, clientId, MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
        config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
        config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
        config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG), config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
        config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), clientDnsLookup, time, true, apiVersions, context);

    _consumerNetworkClient =
        new ConsumerNetworkClient(context, kafkaClient, metadata, time, retryBackoffMs, requestTimeoutMs,
            heartbeatIntervalMs);
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
      _scheduledExecutorService.scheduleWithFixedDelay(runnable, 0, 4, TimeUnit.SECONDS);
      LOGGER.info("Scheduled the offset commit service executor.");

    }
  }

  private class OffsetCommitServiceRunnable implements Runnable {
    @Override
    public void run() {
      sendOffsetWithConsumerNetworkClient(_consumerNetworkClient);
    }
  }

  private void sendOffsetWithConsumerNetworkClient(ConsumerNetworkClient consumerNetworkClient) {
    LOGGER.info("Sending Offset Commit Request to get RequestFuture<ClientResponse>.");
    RequestFuture<ClientResponse> clientResponseRequestFuture = consumerNetworkClient.send(new Node(1, "host", 3),
        new OffsetCommitRequest.Builder(new OffsetCommitRequestData()));

    if (clientResponseRequestFuture.isDone()) {
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
    return _isRunning.get();
  }

  /**
   * Implementations of this method must be thread safe and must be blocking.
   */
  @Override
  public void awaitShutdown() {
    try {
      _scheduledExecutorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException interruptedException) {
      LOGGER.error("Thread interrupted when waiting for {} to shutdown.", _serviceName, interruptedException);
    }
    LOGGER.info("{} shutdown completed.", _serviceName);
  }
}
