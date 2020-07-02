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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class OffsetCommitServiceTest {

  @BeforeMethod
  private void startTest() {
    System.out.println("Started " + this.getClass().getSimpleName().toLowerCase() + ".");
  }

  @AfterMethod
  private void finishTest() {
    System.out.println("Finished " + this.getClass().getCanonicalName().toLowerCase() + ".");
  }

  @Test(invocationCount = 2)
  void serviceStartTest() {

    // TODO (@andrewchoi5): implement offset commit service test

  }
}

