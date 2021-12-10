package com.linkedin.xinfra.monitor.services;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Keep track of assignments made in HAMonitoringCoordinator
 */
public class HAMonitoringIdentity {
    private String id;
    private Boolean isLeader;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public HAMonitoringIdentity(
            @JsonProperty("id") String id
    ) {
        this.id = id;
        this.isLeader = false;
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
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isLeader() {
        return this.isLeader;
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }
}
