/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services;

import com.linkedin.xinfra.monitor.common.MbeanAttributeValue;
import com.linkedin.xinfra.monitor.common.Utils;
import com.linkedin.xinfra.monitor.services.configs.StatsdMetricsReporterServiceConfig;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatsdMetricsReporterService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(StatsdMetricsReporterService.class);

  private final String _name;
  private final List<String> _metricNames;
  private final int _reportIntervalSec;
  private final ScheduledExecutorService _executor;
  private final StatsDClient _statsdClient;

  public StatsdMetricsReporterService(Map<String, Object> props, String name) {
    StatsdMetricsReporterServiceConfig config = new StatsdMetricsReporterServiceConfig(props);

    _name = name;
    _metricNames = config.getList(StatsdMetricsReporterServiceConfig.REPORT_METRICS_CONFIG);
    _reportIntervalSec = config.getInt(StatsdMetricsReporterServiceConfig.REPORT_INTERVAL_SEC_CONFIG);
    _executor = Executors.newSingleThreadScheduledExecutor();
    _statsdClient = new NonBlockingStatsDClient(config.getString(StatsdMetricsReporterServiceConfig.REPORT_STATSD_PREFIX),
            config.getString(StatsdMetricsReporterServiceConfig.REPORT_STATSD_HOST),
            config.getInt(StatsdMetricsReporterServiceConfig.REPORT_STATSD_PORT));
  }

  @Override
  public synchronized void start() {
    _executor.scheduleAtFixedRate(() -> {
      try {
        reportMetrics();
      } catch (Exception e) {
        LOG.error(_name + "/StatsdMetricsReporterService failed to report metrics", e);
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
  public void awaitShutdown(long timeout, TimeUnit unit) {
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
    String[] segs = {serviceType, serviceName, attribute};
    return StringUtils.join(segs, ".");
  }

  private void reportMetrics() {
    for (String metricName: _metricNames) {
      String mbeanExpr = metricName.substring(0, metricName.lastIndexOf(":"));
      String attributeExpr = metricName.substring(metricName.lastIndexOf(":") + 1);
      List<MbeanAttributeValue> attributeValues = Utils.getMBeanAttributeValues(mbeanExpr, attributeExpr);

      for (MbeanAttributeValue attributeValue: attributeValues) {
        final String statsdMetricName = generateStatsdMetricName(attributeValue.mbean(), attributeValue.attribute());
        _statsdClient.recordGaugeValue(statsdMetricName, attributeValue.value());
      }
    }
  }
}
