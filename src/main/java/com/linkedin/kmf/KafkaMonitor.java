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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.kmf.apps.App;
import com.linkedin.kmf.services.Service;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the main entry point of the monitor.  It reads the configuration and manages the life cycle of the monitoring
 * applications.
 */
public class KafkaMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMonitor.class);
  public static final String CLASS_NAME_CONFIG = "class.name";
  private static final String METRIC_GROUP_NAME = "kafka-monitor";
  private static final String JMX_PREFIX = "kmf.services";

  /** This is concurrent because healthCheck() can modify this map, but awaitShutdown() can be called at any time by
   * a different thread.
   */
  private final ConcurrentMap<String, App> _apps;
  private final ConcurrentMap<String, Service> _services;
  private final ConcurrentMap<String, Object> _offlineRunnables;
  private final ScheduledExecutorService _executor;
  /** When true start has been called on this instance of Kafka monitor. */
  private final AtomicBoolean _isRunning = new AtomicBoolean(false);

  public KafkaMonitor(Map<String, Map> testProps) throws Exception {
    _apps = new ConcurrentHashMap<>();
    _services = new ConcurrentHashMap<>();

    for (Map.Entry<String, Map> entry : testProps.entrySet()) {
      String name = entry.getKey();
      Map props = entry.getValue();
      if (!props.containsKey(CLASS_NAME_CONFIG))
        throw new IllegalArgumentException(name + " is not configured with " + CLASS_NAME_CONFIG);
      String className = (String) props.get(CLASS_NAME_CONFIG);

      Class<?> cls = Class.forName(className);
      if (App.class.isAssignableFrom(cls)) {
        App test = (App) Class.forName(className).getConstructor(Map.class, String.class).newInstance(props, name);
        _apps.put(name, test);
      } else if (Service.class.isAssignableFrom(cls)) {
        if (className.equals("com.linkedin.kmf.services.ConsumeService")) {
          CompletableFuture<Void> completableFuture = new CompletableFuture<>();
          completableFuture.complete(null);

          Service service = (Service) Class.forName(className).getConstructor(Map.class, String.class,
              CompletableFuture.class).newInstance(props, name, completableFuture);
          _services.put(name, service);
        } else {
          Service service = (Service) Class.forName(className).getConstructor(Map.class, String.class).newInstance(props, name);
          _services.put(name, service);
        }
      } else {
        throw new IllegalArgumentException(className + " should implement either " + App.class.getSimpleName() + " or " + Service.class.getSimpleName());
      }
    }
    _executor = Executors.newSingleThreadScheduledExecutor();
    _offlineRunnables = new ConcurrentHashMap<>();
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(new MetricConfig(), reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>(1);
    tags.put("name", "kafka-monitor");
    metrics.addMetric(metrics.metricName(
        "offline-runnable-count", METRIC_GROUP_NAME, "The number of Service/App that are not fully running.", tags
        ), (config, now) -> _offlineRunnables.size());
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

    long initialDelaySecond = 5;
    long periodSecond = 5;

    _executor.scheduleAtFixedRate(() -> {
      try {
        checkHealth();
      } catch (Exception e) {
        LOG.error("Failed to check health of tests and services", e);
      }
    }, initialDelaySecond, periodSecond, TimeUnit.SECONDS
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
      LOG.info("USAGE: java [options] " + KafkaMonitor.class.getName() + " config/kafka-monitor.properties");
      return;
    }

    StringBuilder buffer = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new FileReader(args[0].trim()))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (!line.startsWith("#"))
          buffer.append(line);
      }
    }

    @SuppressWarnings("unchecked")
    Map<String, Map> props = new ObjectMapper().readValue(buffer.toString(), Map.class);
    KafkaMonitor kafkaMonitor = new KafkaMonitor(props);
    kafkaMonitor.start();
    LOG.info("KafkaMonitor started.");

    kafkaMonitor.awaitShutdown();
  }

}
