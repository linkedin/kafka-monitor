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
  private volatile KMBaseProducer _producer;
  private volatile ScheduledExecutorService _executor;
  private final ScheduledExecutorService _rebalanceExecutor;
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
  private final TopicRebalancer _topicRebalancer;

  public ProduceService(Map<String, Object> props, String name) throws Exception {
    _name = name;
    _producerPropsOverride = (Map) props.get(ProduceServiceConfig.PRODUCER_PROPS_CONFIG);
    ProduceServiceConfig config = new ProduceServiceConfig(props);
    String zkConnect = config.getString(ProduceServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    _brokerList = config.getString(ProduceServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    double rebalanceThreshold = config.getDouble(ProduceServiceConfig.REBALANCE_EXPECTED_RATIO_CONFIG);
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

    int autoTopicPartitionFactor = config.getInt(ProduceServiceConfig.REBALANCE_PARTITION_FACTOR_CONFIG);
    int autoTopicReplicationFactor = config.getInt(ProduceServiceConfig.AUTO_TOPIC_REPLICATION_FACTOR_CONFIG);
    int existingPartitionCount = Utils.getPartitionNumForTopic(zkConnect, _topic);

    if (existingPartitionCount <= 0) {
      if (config.getBoolean(ProduceServiceConfig.AUTO_TOPIC_CREATION_ENABLED_CONFIG)) {
        _partitionNum.set(
            Utils.createMonitoringTopicIfNotExists(zkConnect, _topic, autoTopicReplicationFactor,
                autoTopicPartitionFactor));
      } else {
        throw new RuntimeException("Can not find valid partition number for topic " + _topic +
            ". Please verify that the topic \"" + _topic + "\" has been created. Ideally the partition number should be" +
            " a multiple of number" +
            " of brokers in the cluster.  Or else configure " + ProduceServiceConfig.AUTO_TOPIC_CREATION_ENABLED_CONFIG +
            " to be true.");
      }
    } else {
      _partitionNum.set(existingPartitionCount);
    }

    initializeProducer(_producerPropsOverride, _producerClass, _threadsNum);

    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put("name", _name);
    _sensors = new ProduceMetrics(metrics, tags);

    if (config.getBoolean(ProduceServiceConfig.REBALANCE_ENABLED_CONFIG)) {
      TopicRebalancer.PartitionsAddedCallback addPartitionCallback = new AddPartitionCallbackImpl();
      //We want a separate executor service so that we don't have multuple threads trying to execute this.
      _rebalanceExecutor = Executors.newScheduledThreadPool(1);
      int rebalanceDelayMs = config.getInt(ProduceServiceConfig.REBALANCE_INTERVAL_MS_CONFIG);
      LOG.info("Scheduling rebalance to run every " + rebalanceDelayMs + "ms.");
      _topicRebalancer =
          new TopicRebalancer(rebalanceThreshold, autoTopicPartitionFactor, _topic, zkConnect,
              addPartitionCallback, rebalanceDelayMs, autoTopicReplicationFactor);
    } else {
      _rebalanceExecutor = null;
      _topicRebalancer = null;
    }
  }

  private void initializeProducer(Map producerPropsOverride, String producerClass, int threadsNum) throws Exception {

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

    _executor = Executors.newScheduledThreadPool(threadsNum);
  }

  @Override
  public void start() {
    if (_running.compareAndSet(false, true)) {
      scheduleProduceRunnables();
      if (_rebalanceExecutor != null) {
        _rebalanceExecutor.scheduleWithFixedDelay(_topicRebalancer, _topicRebalancer.scheduleDurationMs(),
          _topicRebalancer.scheduleDurationMs(), TimeUnit.MILLISECONDS);
      }
      LOG.info(_name + "/ProduceService started");
    }
  }

  private void scheduleProduceRunnables() {
    int partitionNum = _partitionNum.get();
    for (int partition = 0; partition < partitionNum; partition++) {
      //This saves all the indexes across restarts of the produce threads.
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
      if (_rebalanceExecutor != null) {
        _rebalanceExecutor.shutdown();
      }
      _producer.close();
      LOG.info(_name + "/ProduceService stopped");
    }
  }

  @Override
  public void awaitShutdown() {
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
      if (_rebalanceExecutor != null) {
        _rebalanceExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
      }
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
              MetricName partitionRecordsProduced = new MetricName("records-produced-rate-partition-" + partition, METRIC_GROUP_NAME, tags);
              if (!_sensors.metrics.metrics().containsKey(partitionRecordsProduced)) {
                throw new IllegalStateException("Can't find metrics for " + partitionRecordsProduced + ".");
              }
              double recordsProduced = _sensors.metrics.metrics().get(partitionRecordsProduced).value();
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
   * This gets run as a call back when new partitions are added by the rebalancer.
   */
  private class AddPartitionCallbackImpl implements TopicRebalancer.PartitionsAddedCallback {

    @Override
    public synchronized void partitionsAdded(int addedPartitionCount) {
      //TODO: should we exit the monitor if we can't restart the producer runnables?
      _executor.shutdown();
      try {
        _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      _producer.close();
      int nextPartitionStart = _partitionNum.get();
      int newPartitionCount = nextPartitionStart + addedPartitionCount;
      for (int newPartition = nextPartitionStart; newPartition < newPartitionCount; newPartition++) {
        LOG.debug("Adding " + newPartition + " to sensors.");
        _sensors.addPartitionSensors(newPartition);
      }
      _partitionNum.set(newPartitionCount);
      try {
        initializeProducer(_producerPropsOverride, _producerClass, _threadsNum);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      scheduleProduceRunnables();
    }
  }

}