/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services;

import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;


/**
 * Factory class which instantiates a ClusterTopicManipulationService service object.
 */
@SuppressWarnings("rawtypes")
public class ClusterTopicManipulationServiceFactory implements ServiceFactory {

  private final Map _properties;
  private final String _serviceName;

  /**
   * "Class 'ClusterTopicManipulationServiceFactory' is never used" and
   * "Constructor 'ClusterTopicManipulationServiceFactory(java.util.Map, java.lang.String)' is never used"
   * shown as warnings in Intellij IDEA are not true.
   * XinfraMonitor class uses (ServiceFactory) Class.forName(..)
   * .getConstructor(...).newInstance(...) to return Class that's associated
   * with the class or interface with the given string name
   * @param properties config properties
   * @param serviceName name of the service
   */
  public ClusterTopicManipulationServiceFactory(Map properties, String serviceName) {

    _properties = properties;
    _serviceName = serviceName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Service createService() throws Exception {

    AdminClient adminClient = AdminClient.create(_properties);

    return new ClusterTopicManipulationService(_serviceName, adminClient, _properties);
  }
}
