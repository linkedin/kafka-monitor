/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf;

import com.linkedin.kmf.services.Service;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;


@Test
public class KafkaMonitorTest {

  @Test
  public void lifecycleTest() throws Exception {
    KafkaMonitor kafkaMonitor = kafkaMonitor();

    /* Nothing should be started */
    org.testng.Assert.assertEquals(FakeService.startCount.get(), 0);
    org.testng.Assert.assertEquals(FakeService.stopCount.get(), 0);

    /* Should accept but ignore start because start has not been called */
    kafkaMonitor.stop();
    org.testng.Assert.assertEquals(FakeService.stopCount.get(), 0);

    /* Should start */
    kafkaMonitor.start();
    org.testng.Assert.assertEquals(FakeService.startCount.get(), 1);

    /* Should allow start to be called more than once */
    kafkaMonitor.stop();
    kafkaMonitor.stop();
    org.testng.Assert.assertEquals(FakeService.startCount.get(), 1);
    org.testng.Assert.assertEquals(FakeService.stopCount.get(), 1);

    /* Should be allowed to shutdown more than once. */
    kafkaMonitor.awaitShutdown();
    kafkaMonitor.awaitShutdown();
  }

  @Test
  public void awaitShutdownOtherThread() throws Exception {
    final KafkaMonitor kafkaMonitor = kafkaMonitor();
    final AtomicReference<Throwable> error = new AtomicReference<>();

    Thread t = new Thread("test awaitshutdown thread") {
      @Override
      public void run() {
        try {
          kafkaMonitor.awaitShutdown();
        } catch (Throwable t) {
          error.set(t);
        }
      }
    };

    t.start();
    kafkaMonitor.start();
    Thread.sleep(100);
    kafkaMonitor.stop();
    t.join(500);
    org.testng.Assert.assertFalse(t.isAlive());
    org.testng.Assert.assertEquals(error.get(), null);
  }

  private KafkaMonitor kafkaMonitor() throws Exception {
    FakeService.clearCounters();
    Map<String, Map> config = new HashMap<>();
    Map<String, Object> fakeServiceConfig = new HashMap<>();
    fakeServiceConfig.put(KafkaMonitor.CLASS_NAME_CONFIG, FakeService.class.getName());
    config.put("fake-service", fakeServiceConfig);
    return new KafkaMonitor(config);
  }


  static final class FakeService implements Service {

    private static AtomicInteger startCount = new AtomicInteger();
    private static AtomicInteger stopCount = new AtomicInteger();
    private final AtomicBoolean _isRunning = new AtomicBoolean();

    /** required */
    public FakeService(Map<String, Map> config, String serviceInstanceName) {

    }

    private static void clearCounters() {
      startCount.set(0);
      stopCount.set(0);
    }

    @Override
    public void start() {
      _isRunning.compareAndSet(false, true);
      startCount.incrementAndGet();
    }

    @Override
    public synchronized void stop() {
      _isRunning.compareAndSet(true, false);
      stopCount.incrementAndGet();
      notifyAll();
    }

    @Override
    public boolean isRunning() {
      return _isRunning.get();
    }

    @Override
    public synchronized void awaitShutdown() {
      try {
        if (stopCount.get() == 0) {
          wait(3_000);
          if (stopCount.get() == 0) {
            throw new IllegalStateException("Never notified.");
          }
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
