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
import com.linkedin.kmf.producer.BaseProducer;
import com.linkedin.kmf.producer.BaseProducerRecord;
import com.linkedin.kmf.producer.NewProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Measurable;
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
  public static final String METRIC_GROUP_NAME = "produce-metrics";

  private final ProduceMetrics _sensors;
  private final BaseProducer _producer;
  private final ScheduledExecutorService _executor;
  private final int _produceDelayMs;
  private final Map<Integer, AtomicLong> _nextIndexPerPartition;
  private final int _partitionNum;
  private final int _recordSize;
  private final String _topic;
  private final String _producerId;
  private final AtomicBoolean _running;

  public ProduceService(Properties props) throws Exception {
    ServiceConfig config = new ServiceConfig(props);
    String zkConnect = config.getString(ServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    String brokerList = config.getString(ServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    String producerConfigFile = config.getString(ServiceConfig.PRODUCER_PROPS_FILE_CONFIG);
    String producerClass = config.getString(ServiceConfig.PRODUCER_CLASS_CONFIG);
    int threadsNum = config.getInt(ServiceConfig.PRODUCE_THREAD_NUM_CONFIG);
    _topic = config.getString(ServiceConfig.TOPIC_CONFIG);
    _producerId = config.getString(ServiceConfig.PRODUCER_ID_CONFIG);
    _produceDelayMs = config.getInt(ServiceConfig.PRODUCE_RECORD_DELAY_MS_CONFIG);
    _recordSize = config.getInt(ServiceConfig.PRODUCE_RECORD_SIZE_BYTE_CONFIG);
    _partitionNum = Utils.getPartitionNumForTopic(zkConnect, _topic);

    if (_partitionNum < 0)
      throw new RuntimeException("Can not find valid partition number for topic " + _topic + ". Please verify that the topic has been created.");

    Properties producerProps = new Properties();
    if (producerClass.equals(NewProducer.class.getCanonicalName())) {
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
    _producer = (BaseProducer) Class.forName(producerClass).getConstructor(Properties.class).newInstance(producerProps);

    _running = new AtomicBoolean(false);
    _nextIndexPerPartition = new HashMap<>();
    _executor = Executors.newScheduledThreadPool(threadsNum);

    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    _sensors = new ProduceMetrics(metrics);
  }

  @Override
  public void start() {
    if (_running.compareAndSet(false, true)) {
      for (int partition = 0; partition < _partitionNum; partition++) {
        _nextIndexPerPartition.put(partition, new AtomicLong(0));
        _executor.scheduleWithFixedDelay(new ProduceRunnable(partition),
          _produceDelayMs, _produceDelayMs, TimeUnit.MILLISECONDS);
      }
      LOG.info("Produce service started");
    }
  }

  @Override
  public void stop() {
    if (_running.compareAndSet(true, false)) {
      _executor.shutdown();
      _producer.close();
      LOG.info("Produce service stopped");
    }
  }

  @Override
  public void awaitShutdown() {
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
    LOG.info("Produce service shutdown completed");
  }

  @Override
  public boolean isRunning() {
    return _running.get();
  }

  private class ProduceMetrics {
    public final Metrics metrics;
    public final Sensor recordsProduced;
    public final Sensor produceError;
    public final Map<Integer, Sensor> recordsProducedPerPartition;
    public final Map<Integer, Sensor> produceErrorPerPartition;

    public ProduceMetrics(Metrics metrics) {
      this.metrics = metrics;

      recordsProducedPerPartition = new HashMap<>();
      for (int partition = 0; partition < _partitionNum; partition++) {
        Sensor sensor = metrics.sensor("records-produced-partition-" + partition);
        sensor.add(new MetricName("records-produced-rate-partition-" + partition, METRIC_GROUP_NAME), new Rate());
        recordsProducedPerPartition.put(partition, sensor);
      }

      produceErrorPerPartition = new HashMap<>();
      for (int partition = 0; partition < _partitionNum; partition++) {
        Sensor sensor = metrics.sensor("produce-error-partition-" + partition);
        sensor.add(new MetricName("produce-error-rate-partition-" + partition, METRIC_GROUP_NAME), new Rate());
        produceErrorPerPartition.put(partition, sensor);
      }

      recordsProduced = metrics.sensor("records-produced");
      recordsProduced.add(new MetricName("records-produced-rate", METRIC_GROUP_NAME), new Rate());
      recordsProduced.add(new MetricName("records-produced-total", METRIC_GROUP_NAME), new Total());

      produceError = metrics.sensor("produce-error");
      produceError.add(new MetricName("produce-error-rate", METRIC_GROUP_NAME), new Rate());
      produceError.add(new MetricName("produce-error-total", METRIC_GROUP_NAME), new Total());

      metrics.addMetric(new MetricName("produce-availability-avg", METRIC_GROUP_NAME),
        new Measurable() {
          @Override
          public double measure(MetricConfig config, long now) {
            double availabilitySum = 0.0;
            for (int partition = 0; partition < _partitionNum; partition++) {
              double recordsProduced = _sensors.metrics.metrics().get(new MetricName("records-produced-rate-partition-" + partition, METRIC_GROUP_NAME)).value();
              double produceError = _sensors.metrics.metrics().get(new MetricName("produce-error-rate-partition-" + partition, METRIC_GROUP_NAME)).value();
              // If there is no error, error rate sensor may expire and the value may be NaN. Treat NaN as 0 for error rate.
              if (new Double(produceError).isNaN()) {
                produceError = 0;
              }
              // If there is either succeeded or failed produce to a partition, consider its availability as 0.
              if (recordsProduced + produceError > 0) {
                availabilitySum += recordsProduced / (recordsProduced + produceError);
              }
            }
            // Assign equal weight to per-partition availability when calculating overall availability
            return availabilitySum / _partitionNum;
          }
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
        RecordMetadata metadata = _producer.send(record);
        _sensors.recordsProduced.record();
        _sensors.recordsProducedPerPartition.get(_partition).record();

        if (nextIndex == -1) {
          nextIndex = metadata.offset();
        } else {
          nextIndex = nextIndex + 1;
        }
        _nextIndexPerPartition.get(_partition).set(nextIndex);
      } catch (Exception e) {
        _sensors.produceError.record();
        _sensors.produceErrorPerPartition.get(_partition).record();
        LOG.debug("Failed to send message", e);
      }
    }
  }

}