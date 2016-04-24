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

import com.linkedin.kmf.services.configs.CommonServiceConfig;
import com.linkedin.kmf.services.configs.DefaultMetricsReporterServiceConfig;
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

public class DefaultMetricsReporterService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsReporterService.class);

  private final String _name;
  private final List<String> _metricNames;
  private final int _reportIntervalSec;
  private final ScheduledExecutorService _executor;

  public DefaultMetricsReporterService(Properties props) {
    _name = props.containsKey(CommonServiceConfig.SERVICE_NAME_OVERRIDE_CONFIG) ?
      (String) props.get(CommonServiceConfig.SERVICE_NAME_OVERRIDE_CONFIG) : this.getClass().getSimpleName();
    DefaultMetricsReporterServiceConfig config = new DefaultMetricsReporterServiceConfig(props);
    _metricNames = config.getList(DefaultMetricsReporterServiceConfig.REPORT_METRICS_NAMES_CONFIG);
    _reportIntervalSec = config.getInt(DefaultMetricsReporterServiceConfig.REPORT_METRICS_INTERVAL_SEC_CONFIG);
    _executor = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start() {
    if (_metricNames.size() == 0) {
      LOG.info(_name + " is not started because there is not metric to report.");
      return;
    }

    _executor.scheduleAtFixedRate(() -> {
        try {
          reportMetrics();
        } catch (Exception e) {
          LOG.error(_name + " failed to report metrics", e);
        }
      }, _reportIntervalSec, _reportIntervalSec, TimeUnit.SECONDS
    );
    LOG.info(_name + " started");
  }

  @Override
  public void stop() {
    _executor.shutdown();
    LOG.info(_name + " stopped");
  }

  @Override
  public boolean isRunning() {
    return _executor.isTerminated();
  }

  @Override
  public void awaitShutdown() {
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
    LOG.info(_name + " shutdown completed");
  }

  private void reportMetrics() {
    StringBuilder builder = new StringBuilder();
    for (String metricName: _metricNames) {
      builder.append(metricName.substring(metricName.lastIndexOf(":") + 1));
      builder.append(" ");
      builder.append(getMBeanAttributeValue(metricName));
      builder.append(" \t");
    }
    LOG.info(builder.toString());
  }

  private Object getMBeanAttributeValue(String metricName) {
    int splitIndex = metricName.lastIndexOf(':');
    String name = metricName.substring(0, splitIndex);
    String attribute = metricName.substring(splitIndex + 1);
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      return server.getAttribute(new ObjectName(name), attribute);
    } catch (Exception e) {
      return null;
    }
  }

}
