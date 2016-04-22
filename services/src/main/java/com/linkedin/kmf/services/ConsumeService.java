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

import com.linkedin.kmf.common.Protocol;
import com.linkedin.kmf.common.Utils;
import com.linkedin.kmf.consumer.BaseConsumer;
import com.linkedin.kmf.consumer.BaseConsumerRecord;
import com.linkedin.kmf.consumer.NewConsumer;
import com.linkedin.kmf.consumer.OldConsumer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumeService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeService.class);
  private static final String METRIC_GROUP_NAME = "consume-metrics";

  private final ConsumeMetrics _sensors;
  private final BaseConsumer _consumer;
  private final Thread _thread;
  private final int _latencyPercentileMaxMs;
  private final int _latencyPercentileGranularityMs;
  private final AtomicBoolean _running;

  public ConsumeService(Properties props) throws Exception {
    ServiceConfig config = new ServiceConfig(props);
    String topic = config.getString(ServiceConfig.TOPIC_CONFIG);
    String zkConnect = config.getString(ServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    String brokerList = config.getString(ServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    String consumerConfigFile = config.getString(ServiceConfig.CONSUMER_PROPS_FILE_CONFIG);
    String consumerClassName = config.getString(ServiceConfig.CONSUME_CLASS_CONFIG);
    _latencyPercentileMaxMs = config.getInt(ServiceConfig.LATENCY_PERCENTILE_MAX_MS_CONFIG);
    _latencyPercentileGranularityMs = config.getInt(ServiceConfig.LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG);
    _running = new AtomicBoolean(false);

    Properties consumerProps = new Properties();
    if (consumerClassName.equals(NewConsumer.class.getCanonicalName())) {
      consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kmf-consumer-group-" + new Random().nextInt());
      consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
      consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "kmf-consumer");
      Utils.overrideIfDefined(consumerProps, ServiceConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    } else if (consumerClassName.equals(OldConsumer.class.getCanonicalName())){
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kmf-consumer");
      consumerProps.put("auto.commit.enable", "false");
      consumerProps.put("zookeeper.connect", zkConnect);
    }

    if (consumerConfigFile.length() > 0)
      consumerProps = Utils.loadProps(consumerConfigFile, consumerProps);
    _consumer = (BaseConsumer) Class.forName(consumerClassName).getConstructor(String.class, Properties.class).newInstance(topic, consumerProps);

    _thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          consume();
        } catch (Exception e) {
          LOG.error("Consume service failed", e);
        }
      }
    });

    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    _sensors = new ConsumeMetrics(metrics);
  }

  public void consume() {
    Map<Integer, Long> nextIndexes = new HashMap<>();

    while (_running.get()) {
      BaseConsumerRecord record = null;
      try {
        record = _consumer.receive();
      } catch (Exception e) {
        _sensors.consumeError.record();
        LOG.debug("Failed to receive message", e);
        continue;
      }

      if (record == null)
        continue;

      GenericRecord avroRecord = Utils.genericRecordFromJson(record.value());
      if (avroRecord == null) {
        _sensors.consumeError.record();
        continue;
      }
      int partition = record.partition();
      long index = (Long) avroRecord.get(Protocol.INDEX_FIELD.name());
      long currMs = System.currentTimeMillis();
      long prevMs = (Long) avroRecord.get(Protocol.TIME_FIELD.name());
      _sensors.recordsConsumed.record();
      _sensors.bytesConsumed.record(record.value().length());
      _sensors.recordsDelay.record(currMs - prevMs);

      if (index == -1L || !nextIndexes.containsKey(partition)) {
        nextIndexes.put(partition, -1L);
        continue;
      }

      long nextIndex = nextIndexes.get(partition);
      if (nextIndex == -1 || index == nextIndex) {
        nextIndexes.put(partition, index + 1);
      } else if (index < nextIndex) {
        _sensors.recordsDuplicated.record();
      } else if (index > nextIndex) {
        nextIndexes.put(partition, index + 1);
        _sensors.recordsLost.record(index - nextIndex);
      }
    }
  }

  @Override
  public void start() {
    if (_running.compareAndSet(false, true)) {
      _thread.start();
      LOG.info("Consume service started");
    }
  }

  @Override
  public void stop() {
    if (_running.compareAndSet(true, false)) {
      _consumer.close();
      LOG.info("Consume service shutdown stopped");
    }
  }

  @Override
  public void awaitShutdown() {
    try {
      _thread.join();
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
    LOG.info("Consume service shutdown completed");
  }

  @Override
  public boolean isRunning() {
    return _thread.isAlive();
  }

  private class ConsumeMetrics {
    private final Sensor bytesConsumed;
    private final Sensor consumeError;
    private final Sensor recordsConsumed;
    private final Sensor recordsDuplicated;
    private final Sensor recordsLost;
    private final Sensor recordsDelay;

    public ConsumeMetrics(Metrics metrics) {
      bytesConsumed = metrics.sensor("bytes-consumed");
      bytesConsumed.add(new MetricName("bytes-consumed-rate", METRIC_GROUP_NAME), new Rate());

      consumeError = metrics.sensor("consume-error");
      consumeError.add(new MetricName("consume-error-rate", METRIC_GROUP_NAME), new Rate());
      consumeError.add(new MetricName("consume-error-total", METRIC_GROUP_NAME), new Total());

      recordsConsumed = metrics.sensor("records-consumed");
      recordsConsumed.add(new MetricName("records-consumed-rate", METRIC_GROUP_NAME), new Rate());
      recordsConsumed.add(new MetricName("records-consumed-total", METRIC_GROUP_NAME), new Total());

      recordsDuplicated = metrics.sensor("records-duplicated");
      recordsDuplicated.add(new MetricName("records-duplicated-rate", METRIC_GROUP_NAME), new Rate());
      recordsDuplicated.add(new MetricName("records-duplicated-total", METRIC_GROUP_NAME), new Total());

      recordsLost = metrics.sensor("records-lost");
      recordsLost.add(new MetricName("records-lost-rate", METRIC_GROUP_NAME), new Rate());
      recordsLost.add(new MetricName("records-lost-total", METRIC_GROUP_NAME), new Total());

      recordsDelay = metrics.sensor("records-delay");
      recordsDelay.add(new MetricName("records-delay-avg", METRIC_GROUP_NAME), new Avg());
      recordsDelay.add(new MetricName("records-delay-max", METRIC_GROUP_NAME), new Max());

      // There are 2 extra buckets use for values smaller than 0.0 or larger than max, respectively.
      int bucketNum = _latencyPercentileMaxMs / _latencyPercentileGranularityMs + 2;
      int sizeInBytes = 4 * bucketNum;
      recordsDelay.add(new Percentiles(sizeInBytes, _latencyPercentileMaxMs, Percentiles.BucketSizing.CONSTANT,
        new Percentile(new MetricName("records-delay-99th", METRIC_GROUP_NAME), 99.0),
        new Percentile(new MetricName("records-delay-999th", METRIC_GROUP_NAME), 99.9)));
    }

  }

}