/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor;

import com.linkedin.xinfra.monitor.services.ServiceFactory;
import com.linkedin.xinfra.monitor.services.Service;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;


@Test
public class XinfraMonitorTest {

  @Test
  public void lifecycleTest() throws Exception {
    XinfraMonitor xinfraMonitor = xinfraMonitor();

    /* Nothing should be started */
    org.testng.Assert.assertEquals(FakeService.START_COUNT.get(), 0);
    org.testng.Assert.assertEquals(FakeService.STOP_COUNT.get(), 0);

    /* Should accept but ignore start because start has not been called */

    xinfraMonitor.stop();
    org.testng.Assert.assertEquals(FakeService.STOP_COUNT.get(), 0);

    /* Should start */
    xinfraMonitor.start();
    org.testng.Assert.assertEquals(FakeService.START_COUNT.get(), 1);

    /* Should allow start to be called more than once */
    xinfraMonitor.stop();
    xinfraMonitor.stop();
    org.testng.Assert.assertEquals(FakeService.START_COUNT.get(), 1);
    org.testng.Assert.assertEquals(FakeService.STOP_COUNT.get(), 1);


    /* Should be allowed to shutdown more than once. */
    xinfraMonitor.awaitShutdown();
    xinfraMonitor.awaitShutdown();
  }

  @Test
  public void awaitShutdownOtherThread() throws Exception {
    final XinfraMonitor xinfraMonitor = xinfraMonitor();
    final AtomicReference<Throwable> error = new AtomicReference<>();

    Thread t = new Thread("test awaitshutdown thread") {
      @Override
      public void run() {
        try {
          xinfraMonitor.awaitShutdown();
        } catch (Throwable t) {
          error.set(t);
        }
      }
    };

    t.start();
    xinfraMonitor.start();
    Thread.sleep(100);
    xinfraMonitor.stop();
    t.join(500);
    org.testng.Assert.assertFalse(t.isAlive());
    org.testng.Assert.assertEquals(error.get(), null);
  }

  private XinfraMonitor xinfraMonitor() throws Exception {
    FakeService.clearCounters();
    Map<String, Map> config = new HashMap<>();
    Map<String, Object> fakeServiceConfig = new HashMap<>();

    fakeServiceConfig.put(XinfraMonitorConstants.CLASS_NAME_CONFIG, FakeService.class.getName());
    config.put("fake-service", fakeServiceConfig);
    return new XinfraMonitor(config);

  }

  /**
   * Factory class which instantiates a new FakeService service object.
   */
  @SuppressWarnings("rawtypes")
  static final class FakeServiceFactory implements ServiceFactory {

    private final Map _config;
    private final String _serviceInstanceName;

    public FakeServiceFactory(Map config, String serviceInstanceName) {

      this._config = config;
      this._serviceInstanceName = serviceInstanceName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Service createService() throws Exception {

      return new XinfraMonitorTest.FakeService(_config, _serviceInstanceName);

    }
  }

  static final class FakeService implements Service {

    private static final AtomicInteger START_COUNT = new AtomicInteger();
    private static final AtomicInteger STOP_COUNT = new AtomicInteger();
    private final AtomicBoolean _isRunning = new AtomicBoolean();

    /** required */
    public FakeService(Map<String, Map> config, String serviceInstanceName) {

    }

    private static void clearCounters() {
      START_COUNT.set(0);
      STOP_COUNT.set(0);
    }

    @Override
    public void start() {
      _isRunning.compareAndSet(false, true);
      START_COUNT.incrementAndGet();
    }

    @Override
    public synchronized void stop() {
      _isRunning.compareAndSet(true, false);
      STOP_COUNT.incrementAndGet();
      notifyAll();
    }

    @Override
    public boolean isRunning() {
      return _isRunning.get();
    }

    @Override
    public synchronized void awaitShutdown(long timeout, TimeUnit timeUnit) {
      try {
        if (STOP_COUNT.get() == 0) {
          wait(3_000);
          if (STOP_COUNT.get() == 0) {
            throw new IllegalStateException("Never notified.");
          }
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
