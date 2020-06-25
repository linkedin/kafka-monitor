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

import java.util.HashMap;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.SslChannelBuilder;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;


/**
 * Service that monitors the commit offset availability of a partiular Consumer Group.
 */
public class OffsetCommitService implements Service {

  OffsetCommitService() {

  }

  /**
   * The start logic must only execute once.  If an error occurs then the implementer of this class must assume that
   * stop() will be called to clean up.  This method must be thread safe and must assume that stop() may be called
   * concurrently. This can happen if the monitoring application's life cycle is being managed by a container.  Start
   * will only be called once.
   */
  @Override
  public void start() {
    ListenerName listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.SSL);
    SslChannelBuilder sslChannelBuilder = new SslChannelBuilder(Mode.CLIENT, listenerName, true);
    Metrics metrics = new Metrics();
    LogContext context = new LogContext();
    ClusterResourceListeners listeners = new ClusterResourceListeners();
    Time time = Time.SYSTEM;

    Selector selector =
        new Selector(1, 2, 3, metrics, time, "", new HashMap<>(), true, true, sslChannelBuilder, MemoryPool.NONE,
            context);

    Metadata metadata = new Metadata(1, 2, context, listeners);

    KafkaClient kafkaClient =
        new NetworkClient(selector, metadata, "", 2, 3, 4, 5, 6, 7, ClientDnsLookup.DEFAULT, Time.SYSTEM, true,
            new ApiVersions(), context);

    ConsumerNetworkClient consumerNetworkClient =
        new ConsumerNetworkClient(context, kafkaClient, metadata, Time.SYSTEM, 3, 3, 3);

    this.startConsumerNetworkClient(consumerNetworkClient);
  }

  private void startConsumerNetworkClient(ConsumerNetworkClient consumerNetworkClient) {
    RequestFuture<ClientResponse> clientResponseRequestFuture = consumerNetworkClient.send(new Node(1, "host", 3),
        new OffsetCommitRequest.Builder(new OffsetCommitRequestData()));

    if (clientResponseRequestFuture.isDone()) {
      ClientResponse clientResponse = clientResponseRequestFuture.value();

      clientResponse.onComplete();
    }
  }

  /**
   * This may be called multiple times.  This method must be thread safe and must assume that start() may be called
   * concurrently.  This can happen if the monitoring application's life cycle is being managed by a container.
   * Implementations must be non-blocking and should release the resources acquired by the service during start().
   */
  @Override
  public void stop() {

  }

  /**
   * Implementations of this method must be thread safe as it can be called at any time.  Implementations must be
   * non-blocking.
   * @return true if this start() has returned successfully else this must return false.  This must also return false if
   * the service can no longer perform its function.
   */
  @Override
  public boolean isRunning() {
    return false;
  }

  /**
   * Implementations of this method must be thread safe and must be blocking.
   */
  @Override
  public void awaitShutdown() {

  }
}
