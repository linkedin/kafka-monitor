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
import com.linkedin.kmf.partitioner.KMPartitioner;
import com.linkedin.kmf.producer.BaseProducerRecord;
import com.linkedin.kmf.producer.KMBaseProducer;
import com.linkedin.kmf.producer.NewProducer;
import com.linkedin.kmf.services.configs.ProduceServiceConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProduceService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(ProduceService.class);
  private static final String METRIC_GROUP_NAME = "produce-service";
  private static final String[] NON_OVERRIDABLE_PROPERTIES = new String[]{
    ProduceServiceConfig.BOOTSTRAP_SERVERS_CONFIG,
    ProduceServiceConfig.ZOOKEEPER_CONNECT_CONFIG
  };
  private final String _name;
  private final ProduceMetrics _sensors;
  private KMBaseProducer _producer;
  private KMPartitioner _partitioner;
  private ScheduledExecutorService _produceExecutor;
  private final ScheduledExecutorService _handleNewPartitionsExecutor;
  private final int _produceDelayMs;
  private final boolean _sync;
  /** This can be updated while running when new partitions are added to the monitor topic. */
  private final ConcurrentMap<Integer, AtomicLong> _nextIndexPerPartition;
  /** This is the last thing that should be updated after adding new partition sensors. */
  private final AtomicInteger _partitionNum;
  private final int _recordSize;
  private final String _topic;
  private final String _producerId;
  private final AtomicBoolean _running;
  private final String _brokerList;
  private final Map _producerPropsOverride;
  private final String _producerClassName;
  private final int _threadsNum;
  private final boolean _treatZeroThroughputAsUnavailable;
  private final int _latencyPercentileMaxMs;
  private final int _latencyPercentileGranularityMs;
  private final AdminClient _adminClient;
  private static final String KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";

  public ProduceService(Map<String, Object> props, String name) throws Exception {
    _name = name;
    ProduceServiceConfig config = new ProduceServiceConfig(props);
    _brokerList = config.getString(ProduceServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    String producerClass = config.getString(ProduceServiceConfig.PRODUCER_CLASS_CONFIG);
    _latencyPercentileMaxMs = config.getInt(ProduceServiceConfig.LATENCY_PERCENTILE_MAX_MS_CONFIG);
    _latencyPercentileGranularityMs = config.getInt(ProduceServiceConfig.LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG);
    _partitioner = config.getConfiguredInstance(ProduceServiceConfig.PARTITIONER_CLASS_CONFIG, KMPartitioner.class);
    _threadsNum = config.getInt(ProduceServiceConfig.PRODUCE_THREAD_NUM_CONFIG);
    _topic = config.getString(ProduceServiceConfig.TOPIC_CONFIG);
    _producerId = config.getString(ProduceServiceConfig.PRODUCER_ID_CONFIG);
    _produceDelayMs = config.getInt(ProduceServiceConfig.PRODUCE_RECORD_DELAY_MS_CONFIG);
    _recordSize = config.getInt(ProduceServiceConfig.PRODUCE_RECORD_SIZE_BYTE_CONFIG);
    _sync = config.getBoolean(ProduceServiceConfig.PRODUCE_SYNC_CONFIG);
    _treatZeroThroughputAsUnavailable = config.getBoolean(ProduceServiceConfig.PRODUCER_TREAT_ZERO_THROUGHPUT_AS_UNAVAILABLE_CONFIG);
    _partitionNum = new AtomicInteger(0);
    _running = new AtomicBoolean(false);
    _nextIndexPerPartition = new ConcurrentHashMap<>();
    _producerPropsOverride = props.containsKey(ProduceServiceConfig.PRODUCER_PROPS_CONFIG)
      ? (Map) props.get(ProduceServiceConfig.PRODUCER_PROPS_CONFIG) : new HashMap<>();

    for (String property: NON_OVERRIDABLE_PROPERTIES) {
      if (_producerPropsOverride.containsKey(property)) {
        throw new ConfigException("Override must not contain " + property + " config.");
      }
    }

    _adminClient = AdminClient.create(props);

    if (producerClass.equals(NewProducer.class.getCanonicalName()) || producerClass.equals(NewProducer.class.getSimpleName())) {
      _producerClassName = NewProducer.class.getCanonicalName();
    } else {
      _producerClassName = producerClass;
    }

    initializeProducer(props);

    _produceExecutor = Executors.newScheduledThreadPool(_threadsNum, new ProduceServiceThreadFactory());
    _handleNewPartitionsExecutor = Executors.newSingleThreadScheduledExecutor(new HandleNewPartitionsThreadFactory());

    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put("name", _name);
    _sensors = new ProduceMetrics(metrics, tags);
  }

  private void initializeProducer(Map<String, Object> props) throws Exception {
    Properties producerProps = new Properties();
    // Assign default config. This has the lowest priority.
    producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
    producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
    producerProps.put(ProducerConfig.RETRIES_CONFIG, "3");
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE);
    producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS);
    // Assign config specified for ProduceService.
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, _producerId);
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _brokerList);
    // Assign config specified for producer. This has the highest priority.
    producerProps.putAll(_producerPropsOverride);

    if (props.containsKey(ProduceServiceConfig.PRODUCER_PROPS_CONFIG)) {
      props.forEach(producerProps::putIfAbsent);
    }

    _producer = (KMBaseProducer) Class.forName(_producerClassName).getConstructor(Properties.class).newInstance(producerProps);
    LOG.info("{}/ProduceService is initialized.", _name);
  }

  @Override
  public synchronized void start() {
    if (_running.compareAndSet(false, true)) {
      try {
        KafkaFuture<Map<String, TopicDescription>> topicDescriptionsFuture = _adminClient.describeTopics(Collections.singleton(_topic)).all();
        Map<String, TopicDescription> topicDescriptions = topicDescriptionsFuture.get();
        int partitionNum = topicDescriptions.get(_topic).partitions().size();
        initializeStateForPartitions(partitionNum);
        _handleNewPartitionsExecutor.scheduleWithFixedDelay(new NewPartitionHandler(), 1, 30, TimeUnit.SECONDS);
        LOG.info("{}/ProduceService started", _name);
      } catch (InterruptedException | UnknownTopicOrPartitionException | ExecutionException e) {
        LOG.error("Exception occurred while starting produce service: ", e);
      }
    }
  }

  private void initializeStateForPartitions(int partitionNum) {
    Map<Integer, String> keyMapping = generateKeyMappings(partitionNum);
    for (int partition = 0; partition < partitionNum; partition++) {
      String key = keyMapping.get(partition);
      /* This is what preserves sequence numbers across restarts */
      if (!_nextIndexPerPartition.containsKey(partition)) {
        _nextIndexPerPartition.put(partition, new AtomicLong(0));
        _sensors.addPartitionSensors(partition);
      }
      _produceExecutor.scheduleWithFixedDelay(new ProduceRunnable(partition, key), _produceDelayMs, _produceDelayMs, TimeUnit.MILLISECONDS);
    }
    _partitionNum.set(partitionNum);
  }

  private Map<Integer, String> generateKeyMappings(int partitionNum) {
    HashMap<Integer, String> keyMapping = new HashMap<>();

    int nextInt = 0;
    while (keyMapping.size() < partitionNum) {
      String key = Integer.toString(nextInt);
      int partition = _partitioner.partition(key, partitionNum);
      if (!keyMapping.containsKey(partition)) {
        keyMapping.put(partition, key);
      }
      nextInt++;
    }

    return keyMapping;
  }

  @Override
  public synchronized void stop() {
    if (_running.compareAndSet(true, false)) {
      _produceExecutor.shutdown();
      _handleNewPartitionsExecutor.shutdown();
      _producer.close();
      LOG.info("{}/ProduceService stopped.", _name);
    }
  }

  @Override
  public void awaitShutdown() {
    try {
      _produceExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
      _handleNewPartitionsExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted when waiting for {}/ProduceService to shutdown.", _name);
    }
    LOG.info("{}/ProduceService shutdown completed.", _name);
  }


  @Override
  public boolean isRunning() {
    return _running.get() && !_handleNewPartitionsExecutor.isShutdown();
  }

  private class ProduceMetrics {
    public final Metrics _metrics;
    private final Sensor _recordsProduced;
    private final Sensor _produceError;
    private final Sensor _produceDelay;
    private final ConcurrentMap<Integer, Sensor> _recordsProducedPerPartition;
    private final ConcurrentMap<Integer, Sensor> _produceErrorPerPartition;
    private final ConcurrentMap<Integer, Boolean> _produceErrorInLastSendPerPartition;
    private final Map<String, String> _tags;

    public ProduceMetrics(final Metrics metrics, final Map<String, String> tags) {
      _metrics = metrics;
      _tags = tags;

      _recordsProducedPerPartition = new ConcurrentHashMap<>();
      _produceErrorPerPartition = new ConcurrentHashMap<>();
      _produceErrorInLastSendPerPartition = new ConcurrentHashMap<>();

      _recordsProduced = metrics.sensor("records-produced");
      _recordsProduced.add(new MetricName("records-produced-rate", METRIC_GROUP_NAME, "The average number of records per second that are produced", tags), new Rate());
      _recordsProduced.add(new MetricName("records-produced-total", METRIC_GROUP_NAME, "The total number of records that are produced", tags), new Total());

      _produceError = metrics.sensor("produce-error");
      _produceError.add(new MetricName("produce-error-rate", METRIC_GROUP_NAME, "The average number of errors per second", tags), new Rate());
      _produceError.add(new MetricName("produce-error-total", METRIC_GROUP_NAME, "The total number of errors", tags), new Total());

      _produceDelay = metrics.sensor("produce-delay");
      _produceDelay.add(new MetricName("produce-delay-ms-avg", METRIC_GROUP_NAME, "The average delay in ms for produce request", tags), new Avg());
      _produceDelay.add(new MetricName("produce-delay-ms-max", METRIC_GROUP_NAME, "The maximum delay in ms for produce request", tags), new Max());

      // There are 2 extra buckets use for values smaller than 0.0 or larger than max, respectively.
      int bucketNum = _latencyPercentileMaxMs / _latencyPercentileGranularityMs + 2;
      int sizeInBytes = 4 * bucketNum;
      _produceDelay.add(new Percentiles(sizeInBytes, _latencyPercentileMaxMs, Percentiles.BucketSizing.CONSTANT,
          new Percentile(new MetricName("produce-delay-ms-99th", METRIC_GROUP_NAME, "The 99th percentile delay in ms for produce request", tags), 99.0),
          new Percentile(new MetricName("produce-delay-ms-999th", METRIC_GROUP_NAME, "The 99.9th percentile delay in ms for produce request", tags), 99.9),
          new Percentile(new MetricName("produce-delay-ms-9999th", METRIC_GROUP_NAME, "The 99.99th percentile delay in ms for produce request", tags), 99.99)));

      metrics.addMetric(new MetricName("produce-availability-avg", METRIC_GROUP_NAME, "The average produce availability", tags),
        (config, now) -> {
          double availabilitySum = 0.0;
          int partitionNum = _partitionNum.get();
          for (int partition = 0; partition < partitionNum; partition++) {
            double recordsProduced = metrics.metrics().get(metrics.metricName("records-produced-rate-partition-" + partition, METRIC_GROUP_NAME, tags)).value();
            double produceError = metrics.metrics().get(metrics.metricName("produce-error-rate-partition-" + partition, METRIC_GROUP_NAME, tags)).value();
            // If there is no error, error rate sensor may expire and the value may be NaN. Treat NaN as 0 for error rate.
            if (Double.isNaN(produceError) || Double.isInfinite(produceError)) {
              produceError = 0;
            }
            // If there is either succeeded or failed produce to a partition, consider its availability as 0.
            if (recordsProduced + produceError > 0) {
              availabilitySum += recordsProduced / (recordsProduced + produceError);
            } else if (!_treatZeroThroughputAsUnavailable) {
              // If user configures treatZeroThroughputAsUnavailable to be false, a partition's availability
              // is 1.0 as long as there is no exception thrown from producer.
              // This allows kafka admin to exactly monitor the availability experienced by Kafka users which
              // will block and retry for a certain amount of time based on its configuration (e.g. retries, retry.backoff.ms).
              // Note that if it takes a long time for messages to be retries and sent, the latency in the ConsumeService
              // will increase and it will reduce ConsumeAvailability if the latency exceeds consume.latency.sla.ms
              // If timeout is set to more than 60 seconds (the current samples window duration),
              // the error sample might be expired before the next error can be produced.
              // In order to detect offline partition with high producer timeout config, the error status during last
              // send is also checked before declaring 1.0 availability for the partition.
              Boolean lastSendError = _produceErrorInLastSendPerPartition.get(partition);
              if (lastSendError == null || !lastSendError) {
                availabilitySum += 1.0;
              }
            }
          }
          // Assign equal weight to per-partition availability when calculating overall availability
          return availabilitySum / partitionNum;
        });
    }

    void addPartitionSensors(int partition) {
      Sensor recordsProducedSensor = _metrics.sensor("records-produced-partition-" + partition);
      recordsProducedSensor.add(new MetricName("records-produced-rate-partition-" + partition, METRIC_GROUP_NAME,
          "The average number of records per second that are produced to this partition", _tags), new Rate());
      _recordsProducedPerPartition.put(partition, recordsProducedSensor);

      Sensor errorsSensor = _metrics.sensor("produce-error-partition-" + partition);
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
    private final String _key;

    ProduceRunnable(int partition, String key) {
      _partition = partition;
      _key = key;
    }

    public void run() {
      try {
        long nextIndex = _nextIndexPerPartition.get(_partition).get();
        long currMs = System.currentTimeMillis();
        String message = Utils.jsonFromFields(_topic, nextIndex, currMs, _producerId, _recordSize);
        BaseProducerRecord record = new BaseProducerRecord(_topic, _partition, _key, message);
        RecordMetadata metadata = _producer.send(record, _sync);
        _sensors._produceDelay.record(System.currentTimeMillis() - currMs);
        _sensors._recordsProduced.record();
        _sensors._recordsProducedPerPartition.get(_partition).record();
        _sensors._produceErrorInLastSendPerPartition.put(_partition, false);
        if (nextIndex == -1 && _sync) {
          nextIndex = metadata.offset();
        } else {
          nextIndex = nextIndex + 1;
        }
        _nextIndexPerPartition.get(_partition).set(nextIndex);
      } catch (Exception e) {
        _sensors._produceError.record();
        _sensors._produceErrorPerPartition.get(_partition).record();
        _sensors._produceErrorInLastSendPerPartition.put(_partition, true);
        LOG.warn(_name + " failed to send message", e);
      }
    }
  }

  /**
   * This should be periodically run to check for new partitions on the monitor topic.  When new partitions are
   * detected the executor is shutdown, the producer is shutdown, a new producer is created (the old producer does not
   * always become aware of the new partitions), new produce runnables are created on a new executor service, new
   * sensors are added for the new partitions.
   */
  private class NewPartitionHandler implements Runnable {
    public void run() {
      LOG.debug("{}/ProduceService check partition number for topic {}.", _name, _topic);
      try {
        int currentPartitionNum =
            _adminClient.describeTopics(Collections.singleton(_topic)).all().get().get(_topic).partitions().size();
        if (currentPartitionNum <= 0) {
          LOG.info("{}/ProduceService topic {} does not exist.", _name, _topic);
          return;
        } else if (currentPartitionNum == _partitionNum.get()) {
          return;
        }
        LOG.info("{}/ProduceService detected new partitions of topic {}", _name, _topic);
        //TODO: Should the ProduceService exit if we can't restart the producer runnables?
        _produceExecutor.shutdown();
        try {
          _produceExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
        _producer.close();
        try {
          initializeProducer(new HashMap<>());
        } catch (Exception e) {
          LOG.error("Failed to restart producer.", e);
          throw new IllegalStateException(e);
        }
        _produceExecutor = Executors.newScheduledThreadPool(_threadsNum);
        initializeStateForPartitions(currentPartitionNum);
        LOG.info("New partitions added to monitoring.");
      } catch (InterruptedException e) {
        LOG.error("InterruptedException occurred.", e);
      } catch (ExecutionException e) {
        LOG.error("ExecutionException occurred.", e);
      }
    }
  }

  private class ProduceServiceThreadFactory implements ThreadFactory {

    private final AtomicInteger _threadId = new AtomicInteger();
    public Thread newThread(Runnable r) {
      return new Thread(r, _name + "-produce-service-" + _threadId.getAndIncrement());
    }
  }

  private class HandleNewPartitionsThreadFactory implements ThreadFactory {
    public Thread newThread(Runnable r) {
      return new Thread(r, _name + "-produce-service-new-partition-handler");
    }
  }

}
