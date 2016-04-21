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

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

// Jetty server that serves html files.
public class JettyService implements Service {
  private static final Logger log = LoggerFactory.getLogger(JettyService.class);

  private final Server _jettyServer;

  public JettyService(Properties props) {
    ServiceConfig config = new ServiceConfig(props);
    int port = config.getInt(ServiceConfig.PORT_CONFIG);
    _jettyServer = new Server(port);
    ResourceHandler resource_handler = new ResourceHandler();
    resource_handler.setDirectoriesListed(true);
    resource_handler.setWelcomeFiles(new String[]{"index.html"});
    resource_handler.setResourceBase("webapp");
    _jettyServer.setHandler(resource_handler);
  }

  public void start() {
    try {
      _jettyServer.start();
      log.info("Jetty service started at port 8000");
    } catch (Exception e) {
      log.error("Failed to start Jetty server", e);
    }
  }

  public void stop() {
    try {
      _jettyServer.stop();
      log.info("Jetty service stopped");
    } catch (Exception e) {
      log.error("Failed to stop Jetty server", e);
    }
  }

  public boolean isRunning() {
    return _jettyServer.isRunning();
  }

  public void awaitShutdown() {

  }

}
