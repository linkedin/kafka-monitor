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
import com.linkedin.kmf.services.configs.StatsdMetricsReporterServiceConfig;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.linkedin.kmf.common.Utils.getMBeanAttributeValues;

public class StatsdMetricsReporterService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(StatsdMetricsReporterService.class);
  private static final String STATSD_HOST = "STATSD_HOST";
  private static final String STATSD_PORT = "STATSD_PORT";


  private final String _name;
  private final List<String> _metricNames;
  private final int _reportIntervalSec;
  private final ScheduledExecutorService _executor;
  private final StatsDClient _statsdClient;
  private final String _metricNamePrefix;

  public StatsdMetricsReporterService(Map<String, Object> props, String name) {
    StatsdMetricsReporterServiceConfig config = new StatsdMetricsReporterServiceConfig(props);

    _name = name;
    _metricNames = config.getList(StatsdMetricsReporterServiceConfig.REPORT_METRICS_CONFIG);
    _reportIntervalSec = config.getInt(StatsdMetricsReporterServiceConfig.REPORT_INTERVAL_SEC_CONFIG);
    _executor = Executors.newSingleThreadScheduledExecutor();
    _metricNamePrefix = config.getString(StatsdMetricsReporterServiceConfig.REPORT_STATSD_PREFIX);

    if (System.getenv(STATSD_HOST) != null && System.getenv(STATSD_PORT) != null) {
      String statsdHost = System.getenv(STATSD_HOST);
      int statsdPort = Integer.parseInt(System.getenv(STATSD_PORT));

      _statsdClient = new NonBlockingStatsDClient(_metricNamePrefix, statsdHost, statsdPort);
    } else {
      _statsdClient = new NonBlockingStatsDClient(_metricNamePrefix,
              config.getString(StatsdMetricsReporterServiceConfig.REPORT_STATSD_HOST),
              config.getInt(StatsdMetricsReporterServiceConfig.REPORT_STATSD_PORT));
    }
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
            LOG.error(_name + "/StatsdMetricsReporterService failed to report metrics", e);
          }
        }
      }, _reportIntervalSec, _reportIntervalSec, TimeUnit.SECONDS
    );
    LOG.info("{}/StatsdMetricsReporterService started", _name);
  }

  @Override
  public synchronized void stop() {
    _executor.shutdown();
    LOG.info("{}/StatsdMetricsReporterService stopped", _name);
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
      LOG.info("Thread interrupted when waiting for {}/StatsdMetricsReporterService to shutdown", _name);
    }
    LOG.info("{}/StatsdMetricsReporterService shutdown completed", _name);
  }

  private String generateStatsdMetricName(String bean, String attribute) {
    String service = bean.split(":")[1];
    String serviceName = service.split(",")[0].split("=")[1];
    String serviceType = service.split(",")[1].split("=")[1];
    String[] segs = {_metricNamePrefix, serviceType, serviceName, attribute};
    String metricName = StringUtils.join(segs, ".");

    return _metricNamePrefix.isEmpty() ? metricName.substring(1) : metricName;
  }

  private void reportMetrics() {
    for (String metricName: _metricNames) {
      String mbeanExpr = metricName.substring(0, metricName.lastIndexOf(":"));
      String attributeExpr = metricName.substring(metricName.lastIndexOf(":") + 1);
      List<MbeanAttributeValue> attributeValues = getMBeanAttributeValues(mbeanExpr, attributeExpr);

      for (MbeanAttributeValue attributeValue: attributeValues) {
        final String statsdMetricName = generateStatsdMetricName(attributeValue.mbean(), attributeValue.attribute());
        _statsdClient.recordGaugeValue(statsdMetricName, new Double(attributeValue.value()).longValue());
      }
    }
  }
}
