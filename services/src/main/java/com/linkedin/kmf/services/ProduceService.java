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

import com.linkedin.kmf.common.Utils;
import com.linkedin.kmf.services.configs.ProduceServiceConfig;
import com.linkedin.kmf.producer.KMBaseProducer;
import com.linkedin.kmf.producer.BaseProducerRecord;
import com.linkedin.kmf.producer.NewProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ProduceService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(ProduceService.class);
  private static final String METRIC_GROUP_NAME = "produce-metrics";

  private final String _name;
  private final ProduceMetrics _sensors;
  private final KMBaseProducer _producer;
  private final ScheduledExecutorService _executor;
  private final int _produceDelayMs;
  private final boolean _sync;
  private final Map<Integer, AtomicLong> _nextIndexPerPartition;
  private final int _partitionNum;
  private final int _recordSize;
  private final String _topic;
  private final String _producerId;
  private final AtomicBoolean _running;

  public ProduceService(Properties props, String name) throws Exception {
    _name = name;
    ProduceServiceConfig config = new ProduceServiceConfig(props);
    String zkConnect = config.getString(ProduceServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    String brokerList = config.getString(ProduceServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    String producerConfigFile = config.getString(ProduceServiceConfig.PRODUCER_PROPS_FILE_CONFIG);
    String producerClass = config.getString(ProduceServiceConfig.PRODUCER_CLASS_CONFIG);
    int threadsNum = config.getInt(ProduceServiceConfig.PRODUCE_THREAD_NUM_CONFIG);
    _topic = config.getString(ProduceServiceConfig.TOPIC_CONFIG);
    _producerId = config.getString(ProduceServiceConfig.PRODUCER_ID_CONFIG);
    _produceDelayMs = config.getInt(ProduceServiceConfig.PRODUCE_RECORD_DELAY_MS_CONFIG);
    _recordSize = config.getInt(ProduceServiceConfig.PRODUCE_RECORD_SIZE_BYTE_CONFIG);
    _sync = config.getBoolean(ProduceServiceConfig.PRODUCE_SYNC_CONFIG);
    _partitionNum = Utils.getPartitionNumForTopic(zkConnect, _topic);

    if (_partitionNum < 0)
      throw new RuntimeException("Can not find valid partition number for topic " + _topic + ". Please verify that the topic has been created.");

    Properties producerProps = new Properties();
    if (producerClass.equals(NewProducer.class.getCanonicalName()) || producerClass.equals(NewProducer.class.getSimpleName())) {
      producerClass = NewProducer.class.getCanonicalName();
      producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
      producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
      producerProps.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true");
      producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, _producerId);
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    }

    if (producerConfigFile.length() > 0)
      producerProps = Utils.loadProps(producerConfigFile, producerProps);
    _producer = (KMBaseProducer) Class.forName(producerClass).getConstructor(Properties.class).newInstance(producerProps);

    _running = new AtomicBoolean(false);
    _nextIndexPerPartition = new HashMap<>();
    _executor = Executors.newScheduledThreadPool(threadsNum);

    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put("name", _name);
    _sensors = new ProduceMetrics(metrics, tags);
  }

  @Override
  public void start() {
    if (_running.compareAndSet(false, true)) {
      for (int partition = 0; partition < _partitionNum; partition++) {
        _nextIndexPerPartition.put(partition, new AtomicLong(0));
        _executor.scheduleWithFixedDelay(new ProduceRunnable(partition),
          _produceDelayMs, _produceDelayMs, TimeUnit.MILLISECONDS);
      }
      LOG.info(_name + "/ProduceService started");
    }
  }

  @Override
  public void stop() {
    if (_running.compareAndSet(true, false)) {
      _executor.shutdown();
      _producer.close();
      LOG.info(_name + "/ProduceService stopped");
    }
  }

  @Override
  public void awaitShutdown() {
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
    LOG.info(_name + "/ProduceService shutdown completed");
  }

  @Override
  public boolean isRunning() {
    return _running.get();
  }

  private class ProduceMetrics {
    public final Metrics metrics;
    private final Sensor _recordsProduced;
    private final Sensor _produceError;
    private final Map<Integer, Sensor> _recordsProducedPerPartition;
    private final Map<Integer, Sensor> _produceErrorPerPartition;

    public ProduceMetrics(Metrics metrics, Map<String, String> tags) {
      this.metrics = metrics;

      _recordsProducedPerPartition = new HashMap<>();
      for (int partition = 0; partition < _partitionNum; partition++) {
        Sensor sensor = metrics.sensor("records-produced-partition-" + partition);
        sensor.add(new MetricName("records-produced-rate-partition-" + partition, METRIC_GROUP_NAME, tags), new Rate());
        _recordsProducedPerPartition.put(partition, sensor);
      }

      _produceErrorPerPartition = new HashMap<>();
      for (int partition = 0; partition < _partitionNum; partition++) {
        Sensor sensor = metrics.sensor("produce-error-partition-" + partition);
        sensor.add(new MetricName("produce-error-rate-partition-" + partition, METRIC_GROUP_NAME, tags), new Rate());
        _produceErrorPerPartition.put(partition, sensor);
      }

      _recordsProduced = metrics.sensor("records-produced");
      _recordsProduced.add(new MetricName("records-produced-rate", METRIC_GROUP_NAME, tags), new Rate());
      _recordsProduced.add(new MetricName("records-produced-total", METRIC_GROUP_NAME, tags), new Total());

      _produceError = metrics.sensor("produce-error");
      _produceError.add(new MetricName("produce-error-rate", METRIC_GROUP_NAME, tags), new Rate());
      _produceError.add(new MetricName("produce-error-total", METRIC_GROUP_NAME, tags), new Total());

      metrics.addMetric(new MetricName("produce-availability-avg", METRIC_GROUP_NAME, tags),
        (config, now) -> {
          double availabilitySum = 0.0;
          for (int partition = 0; partition < _partitionNum; partition++) {
            double recordsProduced1 = _sensors.metrics.metrics().get(new MetricName("records-produced-rate-partition-" + partition, METRIC_GROUP_NAME, tags)).value();
            double produceError1 = _sensors.metrics.metrics().get(new MetricName("produce-error-rate-partition-" + partition, METRIC_GROUP_NAME, tags)).value();
            // If there is no error, error rate sensor may expire and the value may be NaN. Treat NaN as 0 for error rate.
            if (new Double(produceError1).isNaN()) {
              produceError1 = 0;
            }
            // If there is either succeeded or failed produce to a partition, consider its availability as 0.
            if (recordsProduced1 + produceError1 > 0) {
              availabilitySum += recordsProduced1 / (recordsProduced1 + produceError1);
            }
          }
          // Assign equal weight to per-partition availability when calculating overall availability
          return availabilitySum / _partitionNum;
        });
    }
  }

  private class ProduceRunnable implements Runnable {
    private final int _partition;

    ProduceRunnable(int partition) {
      _partition = partition;
    }

    public void run() {
      try {
        long nextIndex = _nextIndexPerPartition.get(_partition).get();
        String message = Utils.jsonFromFields(_topic, nextIndex, System.currentTimeMillis(), _producerId, _recordSize);
        BaseProducerRecord record = new BaseProducerRecord(_topic, _partition, null, message);
        RecordMetadata metadata = _producer.send(record, _sync);
        _sensors._recordsProduced.record();
        _sensors._recordsProducedPerPartition.get(_partition).record();

        if (nextIndex == -1 && _sync) {
          nextIndex = metadata.offset();
        } else {
          nextIndex = nextIndex + 1;
        }
        _nextIndexPerPartition.get(_partition).set(nextIndex);
      } catch (Exception e) {
        _sensors._produceError.record();
        _sensors._produceErrorPerPartition.get(_partition).record();
        LOG.debug(_name + " failed to send message", e);
      }
    }
  }

}