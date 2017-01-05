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
import com.linkedin.kmf.services.Service;
import com.linkedin.kmf.apps.App;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMonitor.class);
  public static final String CLASS_NAME_CONFIG = "class.name";

  private final Map<String, App> _tests;
  private final Map<String, Service> _services;
  private final ScheduledExecutorService _executor;

  public KafkaMonitor(Map<String, Map> testProps) throws Exception {
    _tests = new HashMap<>();
    _services = new HashMap<>();

    for (Map.Entry<String, Map> entry : testProps.entrySet()) {
      String name = entry.getKey();
      Map props = entry.getValue();
      if (!props.containsKey(CLASS_NAME_CONFIG))
        throw new IllegalArgumentException(name + " is not configured with " + CLASS_NAME_CONFIG);
      String className = (String) props.get(CLASS_NAME_CONFIG);

      if (className.startsWith(App.class.getPackage().getName())) {
        App test = (App) Class.forName(className).getConstructor(Map.class, String.class).newInstance(props, name);
        _tests.put(name, test);
      } else {
        Service service = (Service) Class.forName(className).getConstructor(Map.class, String.class).newInstance(props, name);
        _services.put(name, service);
      }
    }

    _executor = Executors.newSingleThreadScheduledExecutor();
  }

  public void start() {
    for (Map.Entry<String, App> entry: _tests.entrySet()) {
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
      }, 10, 10, TimeUnit.SECONDS
    );
  }

  private void checkHealth() {
    Iterator<Map.Entry<String, App>> testIt = _tests.entrySet().iterator();
    while (testIt.hasNext()) {
      Map.Entry<String, App> entry = testIt.next();
      if (!entry.getValue().isRunning()) {
        LOG.error("Test " + entry.getKey() + " has stopped.");
        testIt.remove();
      }
    }

    Iterator<Map.Entry<String, Service>> serviceIt = _services.entrySet().iterator();
    while (serviceIt.hasNext()) {
      Map.Entry<String, Service> entry = serviceIt.next();
      if (!entry.getValue().isRunning()) {
        LOG.error("Service " + entry.getKey() + " has stopped.");
        serviceIt.remove();
      }
    }
  }

  public void stop() {
    _executor.shutdownNow();
    for (App test: _tests.values())
      test.stop();
    for (Service service: _services.values())
      service.stop();
  }

  public void awaitShutdown() {
    for (App test: _tests.values())
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
    LOG.info("KafkaMonitor started");

    kafkaMonitor.awaitShutdown();
  }

}
