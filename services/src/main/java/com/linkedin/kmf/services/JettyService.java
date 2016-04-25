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

import com.linkedin.kmf.services.configs.JettyServiceConfig;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

// Jetty server that serves html files.
public class JettyService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(JettyService.class);

  private final String _name;
  private final Server _jettyServer;
  private final int _port;

  public JettyService(Properties props, String name) {
    _name = name;
    JettyServiceConfig config = new JettyServiceConfig(props);
    _port = config.getInt(JettyServiceConfig.PORT_CONFIG);
    _jettyServer = new Server(_port);
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setDirectoriesListed(true);
    resourceHandler.setWelcomeFiles(new String[]{"index.html"});
    resourceHandler.setResourceBase("webapp");
    _jettyServer.setHandler(resourceHandler);
  }

  public void start() {
    try {
      _jettyServer.start();
      LOG.info(_name + "/JettyService started at port " + _port);
    } catch (Exception e) {
      LOG.error(_name + "/JettyService failed to start", e);
    }
  }

  public void stop() {
    try {
      _jettyServer.stop();
      LOG.info(_name + "/JettyService stopped");
    } catch (Exception e) {
      LOG.error(_name + "/JettyService failed to stop", e);
    }
  }

  public boolean isRunning() {
    return _jettyServer.isRunning();
  }

  public void awaitShutdown() {

  }

}
