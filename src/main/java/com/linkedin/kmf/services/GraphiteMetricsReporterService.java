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

import com.linkedin.kmf.common.MbeanAttributeValue;
import com.linkedin.kmf.services.configs.GraphiteMetricsReporterServiceConfig;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import net.savantly.graphite.GraphiteClient;
import net.savantly.graphite.GraphiteClientFactory;
import net.savantly.graphite.impl.SimpleCarbonMetric;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphiteMetricsReporterService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(GraphiteMetricsReporterService.class);

  private final String _name;
  private final List<String> _metricNames;
  private final int _reportIntervalSec;
  private final ScheduledExecutorService _executor;
  private final GraphiteClient _graphiteClient;
  private final String _metricNamePrefix;

  public GraphiteMetricsReporterService(Map<String, Object> props, String name)
      throws SocketException, UnknownHostException {
    _name = name;
    GraphiteMetricsReporterServiceConfig config = new GraphiteMetricsReporterServiceConfig(props);
    _metricNames = config.getList(GraphiteMetricsReporterServiceConfig.REPORT_METRICS_CONFIG);
    _reportIntervalSec = config.getInt(GraphiteMetricsReporterServiceConfig.REPORT_INTERVAL_SEC_CONFIG);
    _executor = Executors.newSingleThreadScheduledExecutor();
    _metricNamePrefix = config.getString(GraphiteMetricsReporterServiceConfig.REPORT_GRAPHITE_PREFIX);
    _graphiteClient = GraphiteClientFactory.defaultGraphiteClient(
        config.getString(GraphiteMetricsReporterServiceConfig.REPORT_GRAPHITE_HOST),
        config.getInt(GraphiteMetricsReporterServiceConfig.REPORT_GRAPHITE_PORT));
  }

  @Override
  public synchronized void start() {
    _executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          GraphiteMetricsReporterService.this.reportMetrics();
        } catch (Exception e) {
          LOG.error(_name + "/GraphiteMetricsReporterService failed to report metrics",
              e);
        }
      }
      }, _reportIntervalSec, _reportIntervalSec, TimeUnit.SECONDS
    );
    LOG.info("{}/GraphiteMetricsReporterService started", _name);
  }

  @Override
  public synchronized void stop() {
    _executor.shutdown();
    LOG.info("{}/GraphiteMetricsReporterService stopped", _name);
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
      LOG.info("Thread interrupted when waiting for {}/GraphiteMetricsReporterService to shutdown", _name);
    }
    LOG.info("{}/GraphiteMetricsReporterService shutdown completed", _name);
  }

  private String generateGraphiteMetricName(String bean, String attribute) {
    String service = bean.split(":")[1];
    String serviceName = service.split(",")[0].split("=")[1];
    String serviceType = service.split(",")[1].split("=")[1];
    String[] segs = {_metricNamePrefix, serviceType, serviceName, attribute};
    String metricName = StringUtils.join(segs, ".");
    return _metricNamePrefix.isEmpty() ? metricName.substring(1) : metricName;
  }

  private void reportMetrics() {
    long epoch = System.currentTimeMillis() / 1000;
    for (String metricName: _metricNames) {
      String mbeanExpr = metricName.substring(0, metricName.lastIndexOf(":"));
      String attributeExpr = metricName.substring(metricName.lastIndexOf(":") + 1);
      List<MbeanAttributeValue> attributeValues = com.linkedin.kmf.common.Utils.getMBeanAttributeValues(mbeanExpr, attributeExpr);
      for (MbeanAttributeValue attributeValue: attributeValues) {
        _graphiteClient.saveCarbonMetrics(
            new SimpleCarbonMetric(
                generateGraphiteMetricName(attributeValue.mbean(), attributeValue.attribute()),
                String.valueOf(attributeValue.value()),
                epoch));
      }
    }
  }
}
