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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
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
  private static final String METRIC_GROUP_NAME = "produce-service";

  private final String _name;
  private final ProduceMetrics _sensors;
  private KMBaseProducer _producer;
  private ScheduledExecutorService _executor;
  private final ScheduledExecutorService _checkAddPartitionsExecutor;
  private final int _produceDelayMs;
  private final boolean _sync;
  /** This can be updated while running when new partitions are added to the monitored topic. */
  private final ConcurrentMap<Integer, AtomicLong> _nextIndexPerPartition;
  /** This is the last thing that should be updated after adding new partition sensors. */
  private final AtomicInteger _partitionNum;
  private final int _recordSize;
  private final String _topic;
  private final String _producerId;
  private final AtomicBoolean _running;
  private final String _brokerList;
  private final Map _producerPropsOverride;
  private final String _producerClass;
  private final int _threadsNum;
  private final String _zkConnect;
  private final TopicManagementService _topicRebalanceService;

  public ProduceService(Map<String, Object> props, String name) throws Exception {
    _name = name;
    _producerPropsOverride = (Map) props.get(ProduceServiceConfig.PRODUCER_PROPS_CONFIG);
    ProduceServiceConfig config = new ProduceServiceConfig(props);
    _zkConnect = config.getString(ProduceServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    _brokerList = config.getString(ProduceServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    _producerClass = config.getString(ProduceServiceConfig.PRODUCER_CLASS_CONFIG);
    _threadsNum = config.getInt(ProduceServiceConfig.PRODUCE_THREAD_NUM_CONFIG);
    _topic = config.getString(ProduceServiceConfig.TOPIC_CONFIG);
    _producerId = config.getString(ProduceServiceConfig.PRODUCER_ID_CONFIG);
    _produceDelayMs = config.getInt(ProduceServiceConfig.PRODUCE_RECORD_DELAY_MS_CONFIG);
    _recordSize = config.getInt(ProduceServiceConfig.PRODUCE_RECORD_SIZE_BYTE_CONFIG);
    _sync = config.getBoolean(ProduceServiceConfig.PRODUCE_SYNC_CONFIG);
    _partitionNum = new AtomicInteger(0);
    _running = new AtomicBoolean(false);
    _nextIndexPerPartition = new ConcurrentHashMap<>();

    int autoTopicPartitionFactor = config.getInt(ProduceServiceConfig.PARTITIONS_PER_BROKER_CONFIG);
    int autoTopicReplicationFactor = config.getInt(ProduceServiceConfig.TOPIC_REPLICATION_FACTOR_CONFIG);
    int existingPartitionCount = Utils.getPartitionNumForTopic(_zkConnect, _topic);

    if (existingPartitionCount <= 0) {
      if (config.getBoolean(ProduceServiceConfig.TOPIC_CREATION_ENABLED_CONFIG)) {
        _partitionNum.set(
            Utils.createMonitoringTopicIfNotExists(_zkConnect, _topic, autoTopicReplicationFactor,
                autoTopicPartitionFactor));
      } else {
        throw new RuntimeException("Can not find valid partition number for topic " + _topic +
            ". Please verify that the topic \"" + _topic + "\" has been created. Ideally the partition number should be" +
            " a multiple of number" +
            " of brokers in the cluster.  Or else configure " + ProduceServiceConfig.TOPIC_CREATION_ENABLED_CONFIG +
            " to be true.");
      }
    } else {
      _partitionNum.set(existingPartitionCount);
    }

    initializeProducer(_producerPropsOverride, _producerClass);

    _executor = Executors.newScheduledThreadPool(_threadsNum);
    _checkAddPartitionsExecutor = Executors.newSingleThreadScheduledExecutor();

    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put("name", _name);
    _sensors = new ProduceMetrics(metrics, tags);

    _topicRebalanceService = new TopicManagementService(props, _name + "-topic-management-service");

    LOG.info(_name + ": produce service is initialized.");
  }

  private void initializeProducer(Map producerPropsOverride, String producerClass) throws Exception {

    Properties producerProps = new Properties();

    // Assign default config. This has the lowest priority.
    producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
    producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
    producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
    producerProps.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true");
    producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    if (producerClass.equals(NewProducer.class.getCanonicalName()) || producerClass.equals(NewProducer.class.getSimpleName())) {
      producerClass = NewProducer.class.getCanonicalName();
    }

    // Assign config specified for ProduceService.
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, _producerId);
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _brokerList);

    // Assign config specified for producer. This has the highest priority.
    if (producerPropsOverride != null)
      producerProps.putAll(producerPropsOverride);

    _producer = (KMBaseProducer) Class.forName(producerClass).getConstructor(Properties.class).newInstance(producerProps);
    LOG.info("Producer is initialized.");
  }

  @Override
  public void start() {
    if (_running.compareAndSet(false, true)) {
      scheduleProduceRunnables();
      _checkAddPartitionsExecutor.scheduleWithFixedDelay(new HandleNewPartitions(), 40_000, 40_000, TimeUnit.MILLISECONDS);
      _topicRebalanceService.start();
      LOG.info(_name + "/ProduceService started");
    }
  }

  private void scheduleProduceRunnables() {
    int partitionNum = _partitionNum.get();
    for (int partition = 0; partition < partitionNum; partition++) {
      //This is what preserves sequence numbers across restarts
      if (!_nextIndexPerPartition.containsKey(partition)) {
        _nextIndexPerPartition.put(partition, new AtomicLong(0));
      }
      _executor.scheduleWithFixedDelay(new ProduceRunnable(partition), _produceDelayMs, _produceDelayMs, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void stop() {
    if (_running.compareAndSet(true, false)) {
      _executor.shutdown();
      _checkAddPartitionsExecutor.shutdown();
      _producer.close();
      _topicRebalanceService.stop();
      LOG.info(_name + "/ProduceService stopped");
    }
  }

  @Override
  public void awaitShutdown() {
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
      _checkAddPartitionsExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
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
    private final ConcurrentMap<Integer, Sensor> _recordsProducedPerPartition;
    private final ConcurrentMap<Integer, Sensor> _produceErrorPerPartition;
    private final Map<String, String> _tags;

    public ProduceMetrics(Metrics metrics, final Map<String, String> tags) {
      this.metrics = metrics;
      this._tags = tags;

      _recordsProducedPerPartition = new ConcurrentHashMap<>();
      _produceErrorPerPartition = new ConcurrentHashMap<>();

      int partitionNum = _partitionNum.get();
      for (int partition = 0; partition < partitionNum; partition++) {
        addPartitionSensors(partition);
      }

      _recordsProduced = metrics.sensor("records-produced");
      _recordsProduced.add(new MetricName("records-produced-rate", METRIC_GROUP_NAME, "The average number of records per second that are produced", tags), new Rate());
      _recordsProduced.add(new MetricName("records-produced-total", METRIC_GROUP_NAME, "The total number of records that are produced", tags), new Total());

      _produceError = metrics.sensor("produce-error");
      _produceError.add(new MetricName("produce-error-rate", METRIC_GROUP_NAME, "The average number of errors per second", tags), new Rate());
      _produceError.add(new MetricName("produce-error-total", METRIC_GROUP_NAME, "The total number of errors", tags), new Total());

      metrics.addMetric(new MetricName("produce-availability-avg", METRIC_GROUP_NAME, "The average produce availability", tags),
        new Measurable() {
          @Override
          public double measure(MetricConfig config, long now) {
            double availabilitySum = 0.0;
            int partitionNum = _partitionNum.get();
            for (int partition = 0; partition < partitionNum; partition++) {
              double recordsProduced = _sensors.metrics.metrics().get(new MetricName("records-produced-rate-partition-" + partition, METRIC_GROUP_NAME, tags)).value();
              double produceError = _sensors.metrics.metrics().get(new MetricName("produce-error-rate-partition-" + partition, METRIC_GROUP_NAME, tags)).value();
              // If there is no error, error rate sensor may expire and the value may be NaN. Treat NaN as 0 for error rate.
              if (Double.isNaN(produceError) || Double.isInfinite(produceError)) {
                produceError = 0;
              }
              // If there is either succeeded or failed produce to a partition, consider its availability as 0.
              if (recordsProduced + produceError > 0) {
                availabilitySum += recordsProduced / (recordsProduced + produceError);
              }
            }
            // Assign equal weight to per-partition availability when calculating overall availability
            return availabilitySum / partitionNum;
          }
        }
      );
    }

    void addPartitionSensors(int partition) {
      Sensor recordsProducedSensor = metrics.sensor("records-produced-partition-" + partition);
      recordsProducedSensor.add(new MetricName("records-produced-rate-partition-" + partition, METRIC_GROUP_NAME,
          "The average number of records per second that are produced to this partition", _tags), new Rate());
      _recordsProducedPerPartition.put(partition, recordsProducedSensor);

      Sensor errorsSensor = metrics.sensor("produce-error-partition-" + partition);
      errorsSensor.add(new MetricName("produce-error-rate-partition-" + partition, METRIC_GROUP_NAME,
          "The average number of errors per second when producing to this partition", _tags), new Rate());
      _produceErrorPerPartition.put(partition, errorsSensor);
    }
  }

  /**
   * This creates the records sent to the consumer.
   */
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

  /**
   * This should be periodically run to check for new partitions on the monitored topic.  When new partitions are
   * detected the executor is shutdown, the producer is shutdown, a new producer is created (the old producer does not
   * always become aware of the new partitions), new produce runnables are created on a new executor service, new
   * sensors are added for the new partitions.
   */

  private class HandleNewPartitions implements Runnable {

    public void run() {
      int currentPartitionCount = Utils.getPartitionNumForTopic(_zkConnect, _topic);
      if (currentPartitionCount == _partitionNum.get()) {
        return;
      }
      LOG.info(_name + ": Detected new partitions to monitor.");
      //TODO: Should the ProduceService exit if we can't restart the producer runnables?
      _executor.shutdown();
      try {
        _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      _producer.close();
      int nextPartitionStart = _partitionNum.get();
      int addedPartitionCount = currentPartitionCount - nextPartitionStart;
      int newPartitionCount = nextPartitionStart + addedPartitionCount;
      for (int newPartition = nextPartitionStart; newPartition < newPartitionCount; newPartition++) {
        LOG.debug(_name + ": Adding " + newPartition + " to sensors.");
        _sensors.addPartitionSensors(newPartition);
      }
      _partitionNum.set(newPartitionCount);
      try {
        initializeProducer(_producerPropsOverride, _producerClass);
      } catch (Exception e) {
        LOG.error("Failed to restart producer.", e);
        throw new IllegalStateException(e);
      }
      _executor = Executors.newScheduledThreadPool(_threadsNum);
      scheduleProduceRunnables();
      LOG.info("New partitions added to monitoring.");
    }
  }

}