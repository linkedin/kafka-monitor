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

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommitLatencyService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(ProduceService.class);
  private static final String METRIC_GROUP_NAME = "commit-availability-service";
  public CommitLatencyService(Map<String, Object> props, String name) throws Exception {

  }

  @Override
  public synchronized void start() {
    // TO BE IMPLEMENTED
  }

  @Override
  public synchronized void stop() {
    // TO BE IMPLEMENTED
  }

  @Override
  public void awaitShutdown() {
    // TO BE IMPLEMENTED
  }

  @Override
  public boolean isRunning() {
    return true;
  }



}

