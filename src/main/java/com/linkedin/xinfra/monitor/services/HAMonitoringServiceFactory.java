package com.linkedin.xinfra.monitor.services;

import java.util.Map;


/**
 * Factory class which instantiates a JolokiaService service.
 */
@SuppressWarnings("rawtypes")
public class HAMonitoringServiceFactory implements ServiceFactory {
  public static final String STARTMONITOR = "start.monitor";
  public static final String STOPMONITOR = "stop.monitor";

  private final Map _properties;
  private final String _serviceName;

  public HAMonitoringServiceFactory(Map properties, String serviceName) {
    _properties = properties;
    _serviceName = serviceName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Service createService() throws Exception {
    return new HAMonitoringService(_properties, _serviceName, (Runnable) _properties.get(STARTMONITOR), (Runnable) _properties.get(STOPMONITOR));
  }
}
