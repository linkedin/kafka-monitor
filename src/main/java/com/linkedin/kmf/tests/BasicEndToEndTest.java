/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.tests;

import com.linkedin.kmf.apps.SingleClusterMonitor;
import com.linkedin.kmf.services.ConsumeService;
import com.linkedin.kmf.services.ConsumerFactoryImpl;
import com.linkedin.kmf.services.ProduceService;
import com.linkedin.kmf.services.TopicManagementService;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * @deprecated This class has been deprecated and will be removed in a future release. Please use com.linkedin.kmf.apps.SingleClusterMonitor instead.
 *
 * The BasicEndToEndTest test is intended to monitor the health and performance of kafka brokers. It creates
 * one producer and one consumer with the given configuration, produces messages with increasing integer in the
 * message String, consumes messages, and keeps track of number of lost messages, duplicate messages, e2e latency,
 * throughput, etc.
 *
 * BasicEndToEndTest test exports these metrics via JMX. It also periodically report metrics if DEBUG level logging
 * is enabled. This information can be used by other application to trigger alert when kafka brokers fail. It also
 * allows users to track end-to-end performance, e.g. latency and throughput.
 */

@Deprecated
public class BasicEndToEndTest implements Test {
  private static final Logger LOG = LoggerFactory.getLogger(BasicEndToEndTest.class);

  private final ProduceService _produceService;
  private final ConsumeService _consumeService;
  private final TopicManagementService _topicManagementService;
  private final String _name;

  public BasicEndToEndTest(Map<String, Object> props, String name) throws Exception {
    _name = name;
    _topicManagementService = new TopicManagementService(props, name);
    CompletableFuture<Void> topicPartitionReady = _topicManagementService.topicPartitionResult();
    _produceService = new ProduceService(props, name);
    ConsumerFactoryImpl consumerFactory = new ConsumerFactoryImpl(props);
    _consumeService = new ConsumeService(name, topicPartitionReady, consumerFactory);
  }

  @Override
  public void start() {
    _topicManagementService.start();
    CompletableFuture<Void> topicPartitionResult = _topicManagementService.topicPartitionResult();
    topicPartitionResult.thenRun(() -> {
      try {
        _produceService.start();
        _consumeService.start();
      } finally {
        LOG.info("{} /BasicEndToEndTest started.", _name);
      }
    });
  }

  @Override
  public void stop() {
    _topicManagementService.stop();
    _produceService.stop();
    _consumeService.stop();
    LOG.info("{} /BasicEndToEndTest stopped.", _name);
  }

  @Override
  public boolean isRunning() {
    return _produceService.isRunning() && _consumeService.isRunning() && _topicManagementService.isRunning();
  }

  @Override
  public void awaitShutdown() {
    _topicManagementService.awaitShutdown();
    _produceService.awaitShutdown();
    _consumeService.awaitShutdown();
  }

  public static void main(String[] args) throws Exception {
    SingleClusterMonitor.main(args);
  }
}
