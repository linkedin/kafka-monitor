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
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMonitor.class);

  private final List<Test> _tests;
  private final List<Service> _services;

  public KafkaMonitor(Map<String, Properties> testProps) throws Exception {
    _tests = new ArrayList<>();
    _services = new ArrayList<>();

    for (Map.Entry<String, Properties> entry : testProps.entrySet()) {
      String name = entry.getKey();
      if (name.lastIndexOf('-') != -1)
        name = name.substring(0, name.lastIndexOf('-'));
      Properties props = entry.getValue();

      if (name.startsWith(Test.class.getPackage().getName())) {
        Test test = (Test) Class.forName(name).getConstructor(Properties.class).newInstance(props);
        _tests.add(test);
      } else {
        Service service = (Service) Class.forName(name).getConstructor(Properties.class).newInstance(props);
        _services.add(service);
      }
    }
  }

  public void start() {
    for (Test test : _tests)
      test.start();
    for (Service service : _services)
      service.start();
  }

  public void stop() {
    for (Test test : _tests)
      test.stop();
    for (Service service : _services)
      service.stop();
  }

  public void awaitShutdown() {
    for (Test test : _tests)
      test.awaitShutdown();
    for (Service service : _services)
      service.awaitShutdown();
  }

  public static void main(String[] args) throws Exception {
    if (args.length <= 0) {
      LOG.info("USAGE: java [options] KafkaMonitor kmf.properties");
      return;
    }


    StringBuffer buffer = new StringBuffer();
    try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (!line.startsWith("#"))
          buffer.append(line);
      }
    }

    Map<String,Object> result = new ObjectMapper().readValue(buffer.toString(), Map.class);
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
