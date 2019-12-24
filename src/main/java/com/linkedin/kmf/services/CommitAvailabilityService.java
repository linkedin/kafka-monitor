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

import com.linkedin.kmf.common.DefaultTopicSchema;
import com.linkedin.kmf.common.Utils;
import com.linkedin.kmf.consumer.BaseConsumerRecord;
import com.linkedin.kmf.consumer.KMBaseConsumer;
import com.linkedin.kmf.consumer.NewConsumer;
import com.linkedin.kmf.services.configs.CommonServiceConfig;
import com.linkedin.kmf.services.configs.ConsumeServiceConfig;
import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Boolean.FALSE;

public class CommitAvailabilityService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(CommitAvailabilityService.class);
  private static final String METRIC_GROUP_NAME = "commit-availability-service";
  private static final String NAME = "name";
  private static final int LATENCY_SLA_MS = 1000;
  private static final String TAGS_NAME = "name";
  private static final long TIME_WINDOW_MS = 10000;
  private static final int NUM_SAMPLES = 60;
  private static final long THREAD_SLEEP_MS = 1000;
  private final Map<TopicPartition, OffsetAndMetadata> _offsetsToCommit = new HashMap<>();
  private final Clock _clock;
  private final KMBaseConsumer _consumer;
  private final Map<Integer, Long> _nextIndexes = new HashMap<>();
  private final AtomicBoolean _running;
  private int _commitExceptionCount = 0;
  private Thread _commitThread;
  private CommitAvailabilityMetrics _sensors;
  /**
   * CommitAvailabilityService measures the availability of consume offset commits to the Kafka broker.
   * @param props properties file for configuration.
   */
  public CommitAvailabilityService(Map<String, Object> props)
      throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
             InstantiationException {
    Properties consumerProps = new Properties();
    ConsumeServiceConfig config = new ConsumeServiceConfig(props);
    String topic = config.getString(ConsumeServiceConfig.TOPIC_CONFIG);
    String consumerClassName = config.getString(ConsumeServiceConfig.CONSUMER_CLASS_CONFIG);
    _running = new AtomicBoolean(false);
    // Assign default config. This has the lowest priority.
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "kmf-consumer");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kmf-consumer-group-" + new Random().nextInt());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    String brokerList = config.getString(ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    String zkConnect = config.getString(ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    if (consumerClassName.equals(NewConsumer.class.getCanonicalName()) || consumerClassName.equals(NewConsumer.class.getSimpleName())) {
      consumerClassName = NewConsumer.class.getCanonicalName();
    }
    // Assign config specified for ConsumeService.
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    consumerProps.put(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect);
    Map consumerPropsOverride = props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)
        ? (Map) props.get(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG) : new HashMap<>();
    // Assign config specified for consumer. This has the highest priority.
    consumerProps.putAll(consumerPropsOverride);
    if (props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)) {
      props.forEach(consumerProps::putIfAbsent);
    }
    _consumer = (KMBaseConsumer) Class.forName(consumerClassName).getConstructor(String.class, Properties.class).newInstance(topic, consumerProps);
    _clock = null;
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    MetricConfig metricConfig = new MetricConfig().samples(NUM_SAMPLES).timeWindow(TIME_WINDOW_MS, TimeUnit.MILLISECONDS);
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put(TAGS_NAME, NAME);
    _sensors = new CommitAvailabilityMetrics(metrics, tags, topic);
    _commitThread = new Thread(() -> {
      try {
        commit();
      } catch (Exception e) {
        LOG.error(NAME + "/CommitAvailabilityService commit() failed", e);
      }
    }, NAME + " commit-service");
    _commitThread.setDaemon(true);
  }

  @Override
  public synchronized void start() {
    if (_running.compareAndSet(false, true)) {
      _commitThread.start();
      LOG.info("{}/ConsumeService started.", NAME);
    }
  }

  private void commit() {
    LOG.info("Commiting for {} started.", this.getClass().getSimpleName());
    // Delay 1 second to reduce the chance that consumer creates topic before TopicManagementService
    try {
      Thread.sleep(THREAD_SLEEP_MS);
    } catch (InterruptedException e) {
      LOG.error("Error occurred while sleeping the thread ", e);
    }

    while (_running.get()) {
      BaseConsumerRecord record;
      try {
        record = _consumer.receive();
      } catch (Exception e) {
        LOG.warn(NAME + "/ConsumeService failed to receive record", e);
        // Avoid busy while loop
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          ex.printStackTrace();
        }
        continue;
      }
      if (record == null) continue;
      GenericRecord avroRecord = Utils.genericRecordFromJson(record.value());
      if (avroRecord == null) {
        continue;
      }
      int partition = record.partition();
      long index = (Long) avroRecord.get(DefaultTopicSchema.INDEX_FIELD.name());
      long currMs = System.currentTimeMillis();
      long prevMs = (Long) avroRecord.get(DefaultTopicSchema.TIME_FIELD.name());
      if (currMs - prevMs > LATENCY_SLA_MS)
        if (index == -1L || !_nextIndexes.containsKey(partition)) {
          _nextIndexes.put(partition, -1L);
          continue;
        }
      try {
        _consumer.commitAsync();
      } catch (KafkaException ke) {
        LOG.error("Exception while trying to to async commit.", ke);
        _commitExceptionCount++;
      }
    }
  }

  @Override
  public synchronized void stop() {
    LOG.info("{} stopped.", this.getClass().getSimpleName());
  }

  @Override
  public void awaitShutdown() {

  }

  @Override
  public boolean isRunning() {
    return true;
  }

  private class CommitAvailabilityMetrics {
    private final Sensor _offsetsCommited;
    private CommitAvailabilityMetrics(final Metrics metrics, final Map<String, String> tags, String topicName) {
      _offsetsCommited = metrics.sensor("offsets-commited");
      _offsetsCommited.add(new MetricName("offsets-commited-avg", METRIC_GROUP_NAME, "The average number of offsets per second that are commited", tags), new Rate(
          TimeUnit.SECONDS));
      _offsetsCommited.add(new MetricName("offsets-commited-total", METRIC_GROUP_NAME, "The total number of offsets per second that are commited", tags), new Total(5));
    }
  }
}

