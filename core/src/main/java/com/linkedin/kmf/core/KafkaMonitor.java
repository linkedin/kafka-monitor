/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.kmf.services.Service;
import com.linkedin.kmf.tests.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMonitor.class);
  public static final String CLASS_NAME_CONFIG = "class.name";

  private final Map<String, Test> _tests;
  private final Map<String, Service> _services;
  private final ScheduledExecutorService _executor;

  public KafkaMonitor(Map<String, Properties> testProps) throws Exception {
    _tests = new HashMap<>();
    _services = new HashMap<>();

    for (Map.Entry<String, Properties> entry : testProps.entrySet()) {
      String name = entry.getKey();
      Properties props = entry.getValue();
      if (!props.containsKey(CLASS_NAME_CONFIG))
        throw new IllegalArgumentException(name + " is not configured with " + CLASS_NAME_CONFIG);
      String className = (String) props.get(CLASS_NAME_CONFIG);

      if (className.startsWith(Test.class.getPackage().getName())) {
        Test test = (Test) Class.forName(className).getConstructor(Properties.class, String.class).newInstance(props, name);
        _tests.put(name, test);
      } else {
        Service service = (Service) Class.forName(className).getConstructor(Properties.class, String.class).newInstance(props, name);
        _services.put(name, service);
      }
    }

    _executor = Executors.newSingleThreadScheduledExecutor();
  }

  public void start() {
    for (Map.Entry<String, Test> entry: _tests.entrySet()) {
      entry.getValue().start();
    }
    for (Map.Entry<String, Service> entry: _services.entrySet()) {
      entry.getValue().start();
    }

    _executor.scheduleAtFixedRate(() -> {
        try {
          checkHealth();
        } catch (Exception e) {
          LOG.error("Failed to check health of tests and services", e);
        }
      }, 10, 10, TimeUnit.SECONDS
    );
  }

  private void checkHealth() {
    Iterator<Map.Entry<String, Test>> testIt = _tests.entrySet().iterator();
    while (testIt.hasNext()) {
      Map.Entry<String, Test> entry = testIt.next();
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
    for (Test test: _tests.values())
      test.stop();
    for (Service service: _services.values())
      service.stop();
  }

  public void awaitShutdown() {
    for (Test test: _tests.values())
      test.awaitShutdown();
    for (Service service: _services.values())
      service.awaitShutdown();
  }

  public static void main(String[] args) throws Exception {
    if (args.length <= 0) {
      LOG.info("USAGE: java [options] KafkaMonitor kmf.properties");
      return;
    }


    StringBuilder buffer = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (!line.startsWith("#"))
          buffer.append(line);
      }
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> result = new ObjectMapper().readValue(buffer.toString(), Map.class);
    Map<String, Properties> testProps = new HashMap<>();

    for (Map.Entry<String, Object> entry: result.entrySet()) {
      String testClassName = entry.getKey();
      Properties props = new Properties();
      props.putAll((Map<?, ?>) entry.getValue());
      testProps.put(testClassName, props);
    }

    KafkaMonitor kafkaMonitor = new KafkaMonitor(testProps);
    kafkaMonitor.start();
    LOG.info("KafkaMonitor started");

    kafkaMonitor.awaitShutdown();
  }

}
