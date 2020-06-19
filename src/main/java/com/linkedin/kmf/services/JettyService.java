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

import com.linkedin.kmf.services.configs.JettyServiceConfig;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Jetty server which serves HTML files. (index.html)
 */
public class JettyService implements Service {
  private static final Logger LOGGER = LoggerFactory.getLogger(JettyService.class);

  private final String _name;
  private final Server _jettyServer;
  private final int _port;

  public JettyService(Map<String, Object> props, String name) {

    JettyServiceConfig config = new JettyServiceConfig(props);
    _name = name;
    _port = config.getInt(JettyServiceConfig.PORT_CONFIG);
    _jettyServer = new Server(_port);

    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setDirectoriesListed(true);
    resourceHandler.setWelcomeFiles(new String[]{"index.html"});
    resourceHandler.setResourceBase("webapp");
    _jettyServer.setHandler(resourceHandler);
  }

  @Override
  public synchronized void start() {
    try {
      _jettyServer.start();
      LOGGER.info("{}/JettyService started at port {}", _name, _port);
    } catch (Exception e) {
      LOGGER.error(_name + "/JettyService has failed to start", e);
    }
  }

  @Override
  public synchronized void stop() {
    try {
      _jettyServer.stop();
      LOGGER.info("{}/JettyService stopped.", _name);
    } catch (Exception e) {
      LOGGER.error(_name + "/JettyService failed to stop", e);
    }
  }

  @Override
  public boolean isRunning() {
    return _jettyServer.isRunning();
  }

  @Override
  public void awaitShutdown(long timeout, TimeUnit timeUnit) {

  }
}
