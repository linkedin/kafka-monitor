package com.linkedin.xinfra.monitor.services;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Keep track of assignments made in HAMonitoringCoordinator
 */
public class HAMonitoringIdentity {
  private String _id;
  private Boolean _isLeader;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public HAMonitoringIdentity(
          @JsonProperty("_id") String id
  ) {
    _id = id;
    _isLeader = false;
  }

  public byte[] toBytes() {
    try {
      return MAPPER.writeValueAsBytes(this);
    } catch (Exception e) {
      throw new IllegalArgumentException("Error serializing identity information", e);
    }
  }

  public static HAMonitoringIdentity fromBytes(byte[] jsonData) {
    try {
      return MAPPER.readValue(jsonData, HAMonitoringIdentity.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Error deserializing identity information", e);
    }
  }

  public String getId() {
    return _id;
  }

  public void setId(String id) {
    _id = id;
  }

  public boolean isLeader() {
    return _isLeader;
  }

  public void setLeader(boolean isLeader) {
    _isLeader = isLeader;
  }
}
