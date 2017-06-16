/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf;

import com.linkedin.kmf.apps.App;
import com.linkedin.kmf.services.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is the main entry point of the monitor.  It reads the configuration and manages the life cycle of the monitoring
 * applications.
 */
public class KafkaMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMonitor.class);
  public static final String CLASS_NAME_CONFIG = "class.name";
  private static final String METRIC_GROUP_NAME = "kafka-monitor";
  private static final String JMX_PREFIX = "kmf";

  /** This is concurrent because healthCheck() can modify this map, but awaitShutdown() can be called at any time by
   * a different thread.
   */
  private final ConcurrentMap<String, App> _apps;
  private final ConcurrentMap<String, Service> _services;
  private final ConcurrentMap<String, Object> _offlineRunnables;
  private final ScheduledExecutorService _executor;
  /** When true start has been called on this instance of Kafka monitor. */
  private final AtomicBoolean _isRunning = new AtomicBoolean(false);

  public KafkaMonitor(Config testConfig) throws Exception {
    _apps = new ConcurrentHashMap<>();
    _services = new ConcurrentHashMap<>();

    for (Map.Entry<String, ConfigValue> entry : testConfig.root().entrySet()) {
      String name = entry.getKey();
      Config serviceConfig = testConfig.getConfig(name);
      if (!serviceConfig.hasPath(CLASS_NAME_CONFIG))
        throw new IllegalArgumentException(name + " is not configured with " + CLASS_NAME_CONFIG);
      String className = serviceConfig.getString(CLASS_NAME_CONFIG);

      Class<?> cls = Class.forName(className);
      if (App.class.isAssignableFrom(cls)) {
        App test = (App) Class.forName(className).getConstructor(Config.class, String.class).newInstance(serviceConfig, name);
        _apps.put(name, test);
      } else if (Service.class.isAssignableFrom(cls)) {
        Service service = (Service) Class.forName(className).getConstructor(Config.class, String.class).newInstance(serviceConfig, name);
        _services.put(name, service);
      } else {
        throw new IllegalArgumentException(className + " should implement either " + App.class.getSimpleName() + " or " + Service.class.getSimpleName());
      }
    }
    _executor = Executors.newSingleThreadScheduledExecutor();
    _offlineRunnables = new ConcurrentHashMap<>();
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(new MetricConfig(), reporters, new SystemTime());
    metrics.addMetric(metrics.metricName("offline-runnable-count", METRIC_GROUP_NAME, "The number of Service/App that are not fully running"),
        new Measurable() {
          @Override
          public double measure(MetricConfig config, long now) {
            return _offlineRunnables.size();
          }
        }
    );
  }

  public synchronized void start() {
    if (!_isRunning.compareAndSet(false, true)) {
      return;
    }
    for (Map.Entry<String, App> entry: _apps.entrySet()) {
      entry.getValue().start();
    }
    for (Map.Entry<String, Service> entry: _services.entrySet()) {
      entry.getValue().start();
    }

    _executor.scheduleAtFixedRate(
      new Runnable() {
        @Override
        public void run() {
          try {
            checkHealth();
          } catch (Exception e) {
            LOG.error("Failed to check health of tests and services", e);
          }
        }
      }, 5, 5, TimeUnit.SECONDS
    );
  }

  private void checkHealth() {
    for (Map.Entry<String, App> entry: _apps.entrySet()) {
      if (!entry.getValue().isRunning())
        _offlineRunnables.putIfAbsent(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Service> entry: _services.entrySet()) {
      if (!entry.getValue().isRunning())
        _offlineRunnables.putIfAbsent(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, Object> entry: _offlineRunnables.entrySet()) {
      if (entry.getValue() instanceof App)
        LOG.error("App " + entry.getKey() + " is not fully running.");
      else
        LOG.error("Service " + entry.getKey() + " is not fully running.");
    }

  }

  public synchronized void stop() {
    if (!_isRunning.compareAndSet(true, false)) {
      return;
    }
    _executor.shutdownNow();
    for (App test: _apps.values())
      test.stop();
    for (Service service: _services.values())
      service.stop();
  }

  public void awaitShutdown() {
    for (App test: _apps.values())
      test.awaitShutdown();
    for (Service service: _services.values())
      service.awaitShutdown();
  }

  public static void main(String[] args) throws Exception {
    if (args.length <= 0) {
      LOG.info("USAGE: java [options] " + KafkaMonitor.class.getName() + " config/kafka-monitor.conf");
      return;
    }

    Config config = ConfigFactory.load(args[0]).getConfig("kafka-monitor");

    @SuppressWarnings("unchecked")
    KafkaMonitor kafkaMonitor = new KafkaMonitor(config);
    kafkaMonitor.start();
    LOG.info("KafkaMonitor started");

    kafkaMonitor.awaitShutdown();
  }

}
