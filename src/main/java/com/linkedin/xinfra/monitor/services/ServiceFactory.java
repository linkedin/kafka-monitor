/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services;

/**
 * Factory that instantiates an instance of Xinfra Monitor Service.
 *
 * INFORMATION:
 * "Class 'ClusterTopicManipulationServiceFactory' is never used" and
 * "Constructor 'ClusterTopicManipulationServiceFactory(java.util.Map, java.lang.String)' is never used"
 * shown as warnings in Intellij IDEA are not true.
 * XinfraMonitor class uses (ServiceFactory) Class.forName(..)
 * .getConstructor(...).newInstance(...) to return Class that's associated
 * with the class or interface with the given string name
 */
public interface ServiceFactory {

  /**
   * This method creates a Xinfra Montior Service.
   * @return a Xinrfa Monitor service object
   * @throws Exception that occurs while creating a XM Service
   */
  Service createService() throws Exception;

}
