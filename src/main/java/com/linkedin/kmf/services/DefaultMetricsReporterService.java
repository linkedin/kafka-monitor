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

import com.linkedin.kmf.common.MbeanAttributeValue;
import com.linkedin.kmf.services.configs.DefaultMetricsReporterServiceConfig;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.linkedin.kmf.common.Utils.getMBeanAttributeValues;

public class DefaultMetricsReporterService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsReporterService.class);

  private final String _name;
  private final List<String> _metricNames;
  private final int _reportIntervalSec;
  private final ScheduledExecutorService _executor;

  public DefaultMetricsReporterService(Config serviceConfig, String name) {
    _name = name;
    DefaultMetricsReporterServiceConfig config = new DefaultMetricsReporterServiceConfig(serviceConfig);
    _metricNames = config.getList(DefaultMetricsReporterServiceConfig.REPORT_METRICS_CONFIG);
    _reportIntervalSec = config.getInt(DefaultMetricsReporterServiceConfig.REPORT_INTERVAL_SEC_CONFIG);
    _executor = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public synchronized void start() {
    _executor.scheduleAtFixedRate(
      new Runnable() {
        @Override
        public void run() {
          try {
            reportMetrics();
          } catch (Exception e) {
            LOG.error(_name + "/DefaultMetricsReporterService failed to report metrics", e);
          }
        }
      }, _reportIntervalSec, _reportIntervalSec, TimeUnit.SECONDS
    );
    LOG.info("{}/DefaultMetricsReporterService started", _name);
  }

  @Override
  public synchronized void stop() {
    _executor.shutdown();
    LOG.info("{}/DefaultMetricsReporterService stopped", _name);
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
      LOG.info("Thread interrupted when waiting for {}/DefaultMetricsReporterService to shutdown", _name);
    }
    LOG.info("{}/DefaultMetricsReporterService shutdown completed", _name);
  }

  private void reportMetrics() {
    StringBuilder builder = new StringBuilder();
    for (String metricName: _metricNames) {
      String mbeanExpr = metricName.substring(0, metricName.lastIndexOf(":"));
      String attributeExpr = metricName.substring(metricName.lastIndexOf(":") + 1);
      List<MbeanAttributeValue> attributeValues = getMBeanAttributeValues(mbeanExpr, attributeExpr);
      for (MbeanAttributeValue attributeValue: attributeValues) {
        builder.append(attributeValue.toString());
        builder.append("\n");
      }
    }
    LOG.info(builder.toString());
  }
}
