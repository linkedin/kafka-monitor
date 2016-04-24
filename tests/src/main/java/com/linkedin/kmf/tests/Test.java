/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.tests;

public interface Test {

  public static final String TEST_NAME_OVERRIDE_CONFIG = "test.name.override";
  public static final String TEST_NAME_OVERRIDE_DOC = "If specified, this name is used to identify individual test instance in the log.";

  void start();

  void stop();

  boolean isRunning();

  void awaitShutdown();
}
