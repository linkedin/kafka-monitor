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

import com.linkedin.kmf.consumer.KMBaseConsumer;
import com.linkedin.kmf.services.configs.CommonServiceConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit Testing for the Consume Service Class.
 */
public class ConsumeServiceTest {
  private static final String BROKER_LIST = "localhost:9092";
  private static final String ZK_CONNECT = "localhost:2181";
  private static final String TOPIC = "kafka-monitor-topic-test";
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeService.class);

  @Test
  public void commitAvailabilityTest() {
    // TO DO
  }

  @Test
  public void lifecycleTest() throws ExecutionException, InterruptedException {
    ConsumeService consumeService = consumeService();

    /* Nothing should be started */
    Assert.assertFalse(consumeService.isRunning());
    Assert.assertNotNull(consumeService.getServiceName());

    /* Should accept but ignore start because start has not been called */
    consumeService.stop();
    Assert.assertFalse(consumeService.isRunning());

    /* Should start */
    consumeService.start();
    Assert.assertTrue(consumeService.isRunning());

    /* Should allow start to be called more than once */
    consumeService.stop();
    consumeService.stop();
    Assert.assertFalse(consumeService.isRunning());

    /* Should be allowed to shutdown more than once. */
    consumeService.awaitShutdown();
    consumeService.awaitShutdown();
    Assert.assertFalse(consumeService.isRunning());
  }


  private ConsumeService consumeService() throws ExecutionException, InterruptedException {
    /* Sample ConsumeService instance for unit testing */

    Map<String, Object> fakeProps = new HashMap<>();

    fakeProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
    fakeProps.put(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG, ZK_CONNECT);
    fakeProps.put(CommonServiceConfig.TOPIC_CONFIG, TOPIC);

    ConsumerFactory consumerFactory = new FakeConsumerFactory(fakeProps);
    CompletableFuture<Void> topicPartitionResult = new CompletableFuture<>();
    topicPartitionResult.complete(null);

    return new ConsumeService("name", topicPartitionResult, consumerFactory);
  }

  @Test
  public void awaitShutdownOtherThread() throws Exception {
    final ConsumeService consumeService = consumeService();
    final AtomicReference<Throwable> error = new AtomicReference<>();

    Thread thread = new Thread("test awaitshutdown thread") {
      @Override
      public void run() {
        try {
          consumeService.awaitShutdown();
        } catch (Throwable t) {
          error.set(t);
        }
      }
    };

    thread.start();
    consumeService.start();
    Thread.sleep(100);

    consumeService.stop();
    thread.join(500);

    Assert.assertFalse(thread.isAlive());
    Assert.assertEquals(error.get(), null);

  }

  static final class FakeConsumerFactory implements ConsumerFactory {
    final Map<String, Object> _props;

    /* Required */
    public FakeConsumerFactory(Map<String, Object> props) {
      _props = props;
    }

    @Override
    public AdminClient adminClient() {
      return AdminClient.create(_props);
    }

    @Override
    public int latencySlaMs() {
      return 20000;
    }

    @Override
    public KMBaseConsumer baseConsumer() {
      return null;
    }

    @Override
    public String topic() {
      return TOPIC;
    }

    @Override
    public int latencyPercentileMaxMs() {
      /* LATENCY_PERCENTILE_MAX_MS_CONFIG, */
      return 5000;
    }

    @Override
    public int latencyPercentileGranularityMs() {
      /* LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG */
      return 1;
    }
  }

}
