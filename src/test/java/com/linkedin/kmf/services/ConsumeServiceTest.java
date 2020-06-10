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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * This public class is a Unit Testing class for the Consume Service Class.
 * Also tests for Kafka Monitor Consumer offset commits.
 */
public class ConsumeServiceTest {
  private static final String TOPIC = "kafka-monitor-topic-testing";
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeServiceTest.class);
  private static final String TAGS_NAME = "name";
  private static final String METRIC_GROUP_NAME = "commit-availability-service";
  /* thread start delay in seconds */
  private static final long THREAD_START_DELAY_SECONDS = 4;
  private static final String TAG_NAME_VALUE = "name";
  private static final long MOCK_LAST_COMMITTED_OFFSET = System.currentTimeMillis();
  private static final int PARTITION = 2;
  private static final long FIRST_OFFSET = 2;
  private static final long SECOND_OFFSET = 3;
  private static Map<String, String> tags;

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
    consumeService.startConsumeThreadForTesting();
    Assert.assertTrue(consumeService.isRunning());

    shutdownConsumeService(consumeService);
  }

  @Test
  public void commitAvailabilityTest() throws Exception {
    ConsumeService consumeService = consumeService();
    Metrics metrics = consumeServiceMetrics(consumeService);

    Assert.assertNotNull(metrics.metrics().get(metrics.metricName("offsets-committed-total", METRIC_GROUP_NAME, tags)).metricValue());
    Assert.assertEquals(metrics.metrics().get(metrics.metricName("offsets-committed-total", METRIC_GROUP_NAME, tags)).metricValue(), 0.0);

    /* Should start */
    consumeService.startConsumeThreadForTesting();
    Assert.assertTrue(consumeService.isRunning());

    /* in milliseconds */
    long threadStartDelay = TimeUnit.SECONDS.toMillis(THREAD_START_DELAY_SECONDS);

    /* Thread.sleep safe to do here instead of ScheduledExecutorService
    *  We want to sleep current thread so that consumeService can start running for enough seconds. */
    Thread.sleep(threadStartDelay);
    Assert.assertNotNull(metrics.metrics().get(metrics.metricName("offsets-committed-total", METRIC_GROUP_NAME, tags)).metricValue());
    Assert.assertNotNull(metrics.metrics().get(metrics.metricName("failed-commit-offsets-total", METRIC_GROUP_NAME,
        tags)).metricValue());
    Assert.assertEquals(metrics.metrics().get(metrics.metricName("failed-commit-offsets-total", METRIC_GROUP_NAME, tags)).metricValue(), 0.0);
    Assert.assertNotEquals(metrics.metrics().get(metrics.metricName("offsets-committed-total", METRIC_GROUP_NAME, tags)).metricValue(), 0.0);
    shutdownConsumeService(consumeService);
  }

  @Test
  public void commitLatencyTest() throws Exception {
    CommitLatencyMetrics commitLatencyMetrics = Mockito.mock(CommitLatencyMetrics.class);
    Assert.assertNotNull(commitLatencyMetrics);

    ConsumeService consumeService = consumeService();
    Metrics metrics = consumeServiceMetrics(consumeService);

    Assert.assertNull(metrics.metrics().get(metrics.metricName("commit-offset-latency-ms-avg", METRIC_GROUP_NAME, tags)));
    Assert.assertNull(metrics.metrics().get(metrics.metricName("commit-offset-latency-ms-max", METRIC_GROUP_NAME, tags)));

    /* Should start */
    consumeService.startConsumeThreadForTesting();
    Assert.assertTrue(consumeService.isRunning());

    /* in milliseconds */
    long threadStartDelay = TimeUnit.SECONDS.toMillis(THREAD_START_DELAY_SECONDS);

    /* Thread.sleep safe to do here instead of ScheduledExecutorService
     *  We want to sleep current thread so that consumeService can start running for enough seconds. */
    Thread.sleep(threadStartDelay);

    shutdownConsumeService(consumeService);
  }

  /**
   * Sample ConsumeService instance for unit testing
   * @return Sample ConsumeService object.
   * @throws Exception should the ConsumeService creation fail or throws an error / exception
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
    Mockito.when(kmBaseConsumer.lastCommitted()).thenReturn(MOCK_LAST_COMMITTED_OFFSET);
    Mockito.when(kmBaseConsumer.committed(Mockito.any())).thenReturn(new OffsetAndMetadata(FIRST_OFFSET));
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) {
        OffsetCommitCallback callback = invocationOnMock.getArgument(0);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(new TopicPartition(TOPIC, PARTITION), new OffsetAndMetadata(FIRST_OFFSET));
        callback.onComplete(committedOffsets, null);

        return null;
      }
    }).when(kmBaseConsumer).commitAsync(Mockito.any(OffsetCommitCallback.class));


    /* avro record to KmBaseConsumer record */
    Mockito.when(kmBaseConsumer.receive()).thenReturn(
        new BaseConsumerRecord(TOPIC, PARTITION, SECOND_OFFSET, "key",
            Utils.jsonFromFields(TOPIC, 2, 6000, "producerId", 2)));

    CompletableFuture<Void> topicPartitionResult = new CompletableFuture<>();
    topicPartitionResult.complete(null);

    return new ConsumeService(TAG_NAME_VALUE, topicPartitionResult, consumerFactory);
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
    consumeService.startConsumeThreadForTesting();
    Thread.sleep(100);

    consumeService.stop();
    thread.join(500);

    Assert.assertFalse(thread.isAlive());
    Assert.assertEquals(error.get(), null);

  }

  /**
   * return consume service metrics.
   * @param consumeService ConsumeService object
   * @return consume service metrics
   */
  private Metrics consumeServiceMetrics(ConsumeService consumeService) {
    setup();
    Metrics metrics = consumeService.metrics();
    return metrics;
  }

  /**
   * set up the tags for the metrics
   */
  @BeforeMethod
  public void setup() {
    tags = new HashMap<>();
    tags.put(TAGS_NAME, TAG_NAME_VALUE);
  }

  /**
   * shutdown the consume service.
   * @param consumeService object of ConsumeService
   */
  private void shutdownConsumeService(ConsumeService consumeService) {
    /*
     intentionally attempt stopping twice as such executions shouldn't throw any exceptions.
     Should allow start to be called more than once
    */
    consumeService.stop();
    consumeService.stop();
    Assert.assertFalse(consumeService.isRunning());

    /* Should be allowed to shutdown more than once. */
    consumeService.awaitShutdown();
    consumeService.awaitShutdown();
    Assert.assertFalse(consumeService.isRunning());
  }

}
