/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.xinfra.monitor.apps.App;
import com.linkedin.xinfra.monitor.services.Service;
import com.linkedin.xinfra.monitor.services.ServiceFactory;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class XinfraMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(XinfraMonitor.class);

  /** This is concurrent because healthCheck() can modify this map, but awaitShutdown() can be called at any time by
   * a different thread.
   */
  private final ConcurrentMap<String, App> _apps;
  private final ConcurrentMap<String, Service> _services;
  private final ConcurrentMap<String, Object> _offlineRunnables;
  private final ScheduledExecutorService _executor;
  /** When true start has been called on this instance of Xinfra Monitor. */
  private final AtomicBoolean _isRunning = new AtomicBoolean(false);

  /**
   * XinfraMonitor constructor creates apps and services for each of the individual clusters (properties) that's passed in.
   * For example, if there are 10 clusters to be monitored, then this Constructor will create 10 * num_apps_per_cluster
   * and 10 * num_services_per_cluster.
   * @param allClusterProps the properties of ALL kafka clusters for which apps and services need to be appended.
   * @throws Exception when exception occurs while assigning Apps and Services
   */

  @SuppressWarnings({"rawtypes"})
  public XinfraMonitor(Map<String, Map> allClusterProps) throws Exception {
    _apps = new ConcurrentHashMap<>();
    _services = new ConcurrentHashMap<>();

    for (Map.Entry<String, Map> clusterProperty : allClusterProps.entrySet()) {
      String clusterName = clusterProperty.getKey();
      Map props = clusterProperty.getValue();
      if (!props.containsKey(XinfraMonitorConstants.CLASS_NAME_CONFIG))
        throw new IllegalArgumentException(clusterName + " is not configured with " + XinfraMonitorConstants.CLASS_NAME_CONFIG);
      String className = (String) props.get(XinfraMonitorConstants.CLASS_NAME_CONFIG);

      Class<?> aClass = Class.forName(className);
      if (App.class.isAssignableFrom(aClass)) {
        App clusterApp = (App) Class.forName(className).getConstructor(Map.class, String.class).newInstance(props, clusterName);
        _apps.put(clusterName, clusterApp);
      } else if (Service.class.isAssignableFrom(aClass)) {
        ServiceFactory serviceFactory = (ServiceFactory) Class.forName(className + XinfraMonitorConstants.FACTORY)
            .getConstructor(Map.class, String.class)
            .newInstance(props, clusterName);
        Service service = serviceFactory.createService();
        _services.put(clusterName, service);
      } else {
        throw new IllegalArgumentException(className + " should implement either " + App.class.getSimpleName() + " or " + Service.class.getSimpleName());
      }
    }
    _executor = Executors.newSingleThreadScheduledExecutor();
    _offlineRunnables = new ConcurrentHashMap<>();
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(XinfraMonitorConstants.JMX_PREFIX));
    Metrics metrics = new Metrics(new MetricConfig(), reporters, new SystemTime());
    metrics.addMetric(metrics.metricName("offline-runnable-count", XinfraMonitorConstants.METRIC_GROUP_NAME, "The number of Service/App that are not fully running"),
      (config, now) -> _offlineRunnables.size());
  }

  private boolean constructorContainsClass(Constructor<?>[] constructors, Class<?> classObject) {
    for (int n = 0; n < constructors[0].getParameterTypes().length; ++n) {
      if (constructors[0].getParameterTypes()[n].equals(classObject)) {
        return true;
      }
    }
    return false;
  }

  public synchronized void start() throws Exception {
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
        LOG.error("Failed to check health of apps and services", e);
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
    for (App app: _apps.values())
      app.stop();
    for (Service service: _services.values())
      service.stop();
  }

  public void awaitShutdown() {
    for (App app: _apps.values())
      app.awaitShutdown();
    for (Service service: _services.values())
      service.awaitShutdown(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("rawtypes")
  public static void main(String[] args) throws Exception {
    if (args.length <= 0) {
      LOG.info("USAGE: java [options] " + XinfraMonitor.class.getName() + " config/xinfra-monitor.properties");
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
    XinfraMonitor xinfraMonitor = new XinfraMonitor(props);
    xinfraMonitor.start();
    LOG.info("Xinfra Monitor has started.");

    xinfraMonitor.awaitShutdown();
  }

}
