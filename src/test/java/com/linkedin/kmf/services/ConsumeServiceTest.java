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

import com.linkedin.kmf.common.Utils;
import com.linkedin.kmf.consumer.BaseConsumerRecord;
import com.linkedin.kmf.consumer.KMBaseConsumer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit Testing for the Consume Service Class.
 * Also tests for Kafka Monitor Consumer offset commits.
 */
public class ConsumeServiceTest {
  private static final String TOPIC = "kafka-monitor-topic-testing";
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeServiceTest.class);
  private CommitAvailabilityMetrics _commitAvailabilityMetrics;
  private static final String TAGS_NAME = "name";
  private static final String METRIC_GROUP_NAME = "commit-availability-service";
  private static Boolean testCommittedSuccessfully = false;

  @Test
  public void lifecycleTest() throws Exception {
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

  @Test
  public void commitAvailabilityTest() throws Exception {
    ConsumeService consumeService = consumeService();

    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(Service.JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    String name = "name";
    Map<String, String> tags = new HashMap<>();
    tags.put(TAGS_NAME, name);

    _commitAvailabilityMetrics = new CommitAvailabilityMetrics(metrics, tags);
    Assert.assertNotNull(_commitAvailabilityMetrics._offsetsCommitted.name());
    Assert.assertNotNull(metrics.metrics().get(metrics.metricName("offsets-committed-total", METRIC_GROUP_NAME, tags)).metricValue());
    Assert.assertEquals(metrics.metrics().get(metrics.metricName("offsets-committed-total", METRIC_GROUP_NAME, tags)).metricValue(), 0.0);

    /* Should start */
    consumeService.start();
    Assert.assertTrue(consumeService.isRunning());

    long threadStartDelay = 2000;

    /* Thread.sleep safe to do here instead of ScheduledExecutorService
    *  We want to sleep current thread so that consumeService can start running for enough seconds. */
    Thread.sleep(threadStartDelay);
    Assert.assertNotNull(metrics.metrics().get(metrics.metricName("offsets-committed-total", METRIC_GROUP_NAME, tags)).metricValue());
    Assert.assertTrue(testCommittedSuccessfully);

    consumeService.stop();
    consumeService.stop();

    consumeService.awaitShutdown();
  }

  /**
   * Sample ConsumeService instance for unit testing
   * @return Sample ConsumeService object.
   * @throws Exception
   */
  private ConsumeService consumeService() throws Exception {
    LOG.info("Creating an instance of Consume Service for testing..");

    ConsumerFactory consumerFactory = Mockito.mock(ConsumerFactory.class);
    AdminClient adminClient = Mockito.mock(AdminClient.class);
    KMBaseConsumer kmBaseConsumer = Mockito.mock(KMBaseConsumer.class);

    Mockito.when(consumerFactory.adminClient()).thenReturn(adminClient);
    Mockito.when(consumerFactory.latencySlaMs()).thenReturn(20000);
    Mockito.when(consumerFactory.baseConsumer()).thenReturn(kmBaseConsumer);
    Mockito.when(consumerFactory.topic()).thenReturn(TOPIC);

    /* LATENCY_PERCENTILE_MAX_MS_CONFIG, */
    Mockito.when(consumerFactory.latencyPercentileMaxMs()).thenReturn(5000);

    /* LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG */
    Mockito.when(consumerFactory.latencyPercentileGranularityMs()).thenReturn(1);

    /* define return value */
    long mockLastCommittedOffset = 0;
    Mockito.when(kmBaseConsumer.lastCommitted()).thenReturn(mockLastCommittedOffset);
    Mockito.doAnswer(new Answer() {
      @Override
      public String answer(InvocationOnMock invocation) {
        testCommittedSuccessfully = true;
        return "Mock Offset Committed Successfully.";

      }
    }).when(kmBaseConsumer).commitAsync(Mockito.any(OffsetCommitCallback.class));

    /* avro record to KmBaseConsumer record */
    Mockito.when(kmBaseConsumer.receive()).thenReturn(
        new BaseConsumerRecord(TOPIC, 2, 3, "key",
            Utils.jsonFromFields(TOPIC, 2, 3, "producerId", 2)));

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

}
