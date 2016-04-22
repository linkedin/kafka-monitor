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

import org.jolokia.jvmagent.JolokiaServer;
import org.jolokia.jvmagent.JvmAgentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

// Jolokia server allows user to query jmx metric value with HTTP request
public class JolokiaService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(JettyService.class);

  private final JolokiaServer _jolokiaServer;
  private final AtomicBoolean _isRunning;

  public JolokiaService(Properties props) throws Exception {
    _jolokiaServer = new JolokiaServer(new JvmAgentConfig("host=*,port=8778"), false);
    _isRunning = new AtomicBoolean(false);
  }

  public void start() {
    if (_isRunning.compareAndSet(false, true)) {
      _jolokiaServer.start();
      LOG.info("Jolokia service started at port 8778");
    }
  }

  public void stop() {
    if (_isRunning.compareAndSet(true, false)) {
      _jolokiaServer.stop();
      ;
      LOG.info("Jolokia service stopped");
    }
  }

  public boolean isRunning() {
    return _isRunning.get();
  }

  public void awaitShutdown() {

  }

}
