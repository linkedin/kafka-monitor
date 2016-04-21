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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricsExportService implements Service {
  private static final Logger log = LoggerFactory.getLogger(MetricsExportService.class);

  private final List<String> _metricNames;
  private final int _reportIntervalSec;
  private final ScheduledExecutorService _reportExecutor;

  public MetricsExportService(Properties props) {
    ServiceConfig config = new ServiceConfig(props);
    _metricNames = config.getList(ServiceConfig.EXPORT_METRICS_NAMES_CONFIG);
    _reportIntervalSec = config.getInt(ServiceConfig.EXPORT_METRICS_REPORT_INTERVAL_SEC_CONFIG);
    _reportExecutor = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start() {
    if (_metricNames.size() == 0) {
      log.info("MetricsExport service is not started because metric-to-export list is empty.");
      return;
    }

    _reportExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          exportMetrics();
        } catch (Exception e) {
          log.error("Failed to export metrics", e);
        }
      }
    }, _reportIntervalSec, _reportIntervalSec, TimeUnit.SECONDS);
    log.info("MetricsExport service started");
  }

  public void exportMetrics() {
    StringBuilder builder = new StringBuilder();
    for (String metricName: _metricNames) {
      String attributeName = metricName.substring(metricName.lastIndexOf(":") + 1);
      builder.append(attributeName + " " + getMBeanAttributeValue(metricName) + "\t");
    }
    log.info(builder.toString());
  }

  @Override
  public void stop() {
    _reportExecutor.shutdown();
    log.info("MetricsExport service stoppped");
  }

  @Override
  public boolean isRunning() {
    return _reportExecutor.isTerminated();
  }

  @Override
  public void awaitShutdown() {
    try {
      _reportExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
    log.info("ExportMetrics service shutdown completed");
  }

  public Object getMBeanAttributeValue(String metricName) {
    int splitIndex = metricName.lastIndexOf(':');
    String name = metricName.substring(0, splitIndex);
    String attribute = metricName.substring(splitIndex + 1);
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      Object object = server.getAttribute(new ObjectName(name), attribute);
      return object;
    } catch (Exception e) {
      return null;
    }
  }

}
