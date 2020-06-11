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

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jolokia.jvmagent.JolokiaServer;
import org.jolokia.jvmagent.JvmAgentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jolokia server allows user to query jmx metric value with HTTP request.
 */
public class JolokiaService implements Service {
  private static final Logger LOGGER = LoggerFactory.getLogger(JolokiaService.class);

  private final String _name;
  private final JolokiaServer _jolokiaServer;
  private final AtomicBoolean _isRunning;

  public JolokiaService(Map<String, Object> unused, String name) throws Exception {
    _name = name;
    _jolokiaServer = new JolokiaServer(new JvmAgentConfig("host=*,port=8778"), false);
    _isRunning = new AtomicBoolean(false);
  }

  @Override
  public synchronized void start() {
    if (_isRunning.compareAndSet(false, true)) {
      _jolokiaServer.start();
      LOGGER.info("{}/JolokiaService started at port 8778", _name);
    }
  }

  @Override
  public synchronized void stop() {
    if (_isRunning.compareAndSet(true, false)) {
      _jolokiaServer.stop();
      LOGGER.info("{}/JolokiaService stopped", _name);
    }
  }

  @Override
  public boolean isRunning() {
    return _isRunning.get();
  }

  @Override
  public void awaitShutdown() {

  }

}
