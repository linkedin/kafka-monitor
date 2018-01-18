package com.linkedin.kmf.services;

import static com.linkedin.kmf.common.Utils.getMBeanAttributeValues;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.kmf.common.MbeanAttributeValue;
import com.linkedin.kmf.common.Utils;
import com.linkedin.kmf.services.configs.KafkaMetricsReporterServiceConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2018/01/18
 * Time: 11:30 AM
 * Desc:
 */
public class KafkaMetricsReporterService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsReporterService.class);

    private final String _name;
    private final List<String> _metricsNames;
    private final int _reportIntervalSec;
    private final ScheduledExecutorService _executor;

    private KafkaProducer<String, String> _producer;
    private final String _producerId;
    private final String _brokerList;
    private final String _zkConnect;
    private final String _topic;

    private final ObjectMapper parser = new ObjectMapper();

    public KafkaMetricsReporterService(Map<String, Object> props, String name) throws Exception {
        _name = name;
        KafkaMetricsReporterServiceConfig config = new KafkaMetricsReporterServiceConfig(props);
        _metricsNames = config.getList(KafkaMetricsReporterServiceConfig.REPORT_METRICS_CONFIG);
        _reportIntervalSec = config.getInt(KafkaMetricsReporterServiceConfig.REPORT_INTERVAL_SEC_CONFIG);
        _executor = Executors.newSingleThreadScheduledExecutor();

        _brokerList = config.getString(KafkaMetricsReporterServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
        _producerId = config.getString(KafkaMetricsReporterServiceConfig.PRODUCER_ID_CONFIG);
        initializeProducer();

        _zkConnect = config.getString(KafkaMetricsReporterServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
        _topic = config.getString(KafkaMetricsReporterServiceConfig.TOPIC_CONFIG);
        Utils.createTopicIfNotExists(_zkConnect, _topic, 1, 1, new Properties());
    }

    @Override
    public synchronized void start() {
        _executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    reportMetrics();
                } catch (Exception e) {
                    LOG.error(_name + "/KafkaMetricsReporterService failed to report metrics", e);
                }
            }
        }, _reportIntervalSec, _reportIntervalSec, TimeUnit.SECONDS);
        LOG.info("{}/KafkaMetricsReporterService started", _name);
    }

    @Override
    public void stop() {
        _executor.shutdown();
        _producer.close();
        LOG.info("{}/KafkaMetricsReporterService stopped", _name);
    }

    @Override
    public boolean isRunning() {
        return !_executor.isShutdown();
    }

    @Override
    public void awaitShutdown() {
        try {
            _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.info("Thread interrupted when waiting for {}/KafkaMetricsReporterService to shutdown", _name);
        }
        LOG.info("{}/KafkaMetricsReporterService shutdown completed", _name);
    }

    private void initializeProducer() throws Exception {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, "3");
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Long.MAX_VALUE));
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, _producerId);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _brokerList);
        _producer = new KafkaProducer<>(producerProps);
    }

    private void reportMetrics() {
        Map<String, String> metrics = new HashMap<>();
        for(String metricName: _metricsNames) {
            String mbeanExpr = metricName.substring(0, metricName.lastIndexOf(":"));
            String attributeExpr = metricName.substring(metricName.lastIndexOf(":") + 1);
            List<MbeanAttributeValue> attributeValues = getMBeanAttributeValues(mbeanExpr, attributeExpr);
            for(MbeanAttributeValue attributeValue: attributeValues) {
                String metric = attributeValue.toString();
                String key = metric.substring(0, metric.lastIndexOf("="));
                String val = metric.substring(metric.lastIndexOf("=") + 1);
                metrics.put(key, val);
            }
        }
        try {
            LOG.info("Kafka Metrics Reporter sending metrics = " + parser.writerWithDefaultPrettyPrinter().writeValueAsString(metrics));
            _producer.send(new ProducerRecord<String, String>(_topic, parser.writeValueAsString(metrics)));
        } catch (JsonProcessingException e) {
            LOG.warn("unsupported json format: " + metrics, e);
        }
    }
}
