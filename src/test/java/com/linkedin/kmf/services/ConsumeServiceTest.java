/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services;

import com.linkedin.kmf.services.configs.CommonServiceConfig;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;


public class ConsumeServiceTest {
  private static final String BROKER_LIST = "localhost:9092";
  private static final String ZK_CONNECT = "localhost:2181";
  private static final String TOPIC = "kafka-monitor-topic-test";

  @Test
  public void lifecycleTest()
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException,
             InvocationTargetException {

    ConsumeService consumeService = consumeService();

    /* Nothing should be started */
    assertEquals(FakeConsumerFactory.startCount.get(), 0);
    assertEquals(FakeConsumerFactory.stopCount.get(), 0);

    /* Should accept but ignore start because start has not been called */
    consumeService.stop();
    assertEquals(FakeConsumerFactory.stopCount.get(), 0);

    /* Should start */
    consumeService.start();
    assertTrue(consumeService.isRunning());

    /* Should allow start to be called more than once */
    consumeService.stop();
    consumeService.stop();
    assertFalse(consumeService.isRunning());

    /* Should be allowed to shutdown more than once. */
    consumeService.awaitShutdown();
    consumeService.awaitShutdown();
    assertFalse(consumeService.isRunning());
  }


  private ConsumeService consumeService()
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
             IllegalAccessException {
    ConsumeServiceTest.FakeConsumerFactory.clearCounters();

    Map<String, Object> fakeProps = new HashMap<>();
    fakeProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
    fakeProps.put(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG, ZK_CONNECT);
    fakeProps.put(CommonServiceConfig.TOPIC_CONFIG, TOPIC);
    ConsumerFactory consumerFactory = new ConsumerFactory(fakeProps);
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
    org.testng.Assert.assertFalse(thread.isAlive());
    assertEquals(error.get(), null);
  }

  static final class FakeConsumerFactory {
    private static AtomicInteger startCount = new AtomicInteger();
    private static AtomicInteger stopCount = new AtomicInteger();
    private final AtomicBoolean _isRunning = new AtomicBoolean();

    /** required */
    public FakeConsumerFactory(Map<String, Map> config, String serviceInstanceName) {
    }

    private static void clearCounters() {
      startCount.set(0);
      stopCount.set(0);
    }
  }

}
