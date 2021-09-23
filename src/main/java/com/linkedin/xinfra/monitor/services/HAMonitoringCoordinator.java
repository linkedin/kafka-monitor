package com.linkedin.xinfra.monitor.services;

import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;

class HAMonitoringProtocol {
    public static ByteBuffer serializeMetadata(HAMonitoringIdentity id) {
        return ByteBuffer.wrap(id.toBytes());
    }

    public static HAMonitoringIdentity deserializeMetadata(ByteBuffer data) {
        return HAMonitoringIdentity.fromBytes(data.array());
    }
}

public class HAMonitoringCoordinator extends AbstractCoordinator {
    private final Runnable startMonitor;
    private final Runnable stopMonitor;
    private HAMonitoringIdentity identity;
    private final Logger LOG;

    public HAMonitoringCoordinator(GroupRebalanceConfig groupRebalanceConfig,
                                     LogContext logContext,
                                     ConsumerNetworkClient client,
                                     Metrics metrics,
                                     String metricGrpPrefix,
                                     Time time,
                                     Runnable start,
                                     Runnable stop,
                                     HAMonitoringIdentity id) {
        super(groupRebalanceConfig,
                logContext,
                client,
                metrics,
                metricGrpPrefix,
                time);

        this.startMonitor = start;
        this.stopMonitor = stop;
        this.identity = id;
        this.LOG = logContext.logger(HAMonitoringCoordinator.class);
    }

    @Override
    public String protocolType() {
        return "xinfraleaderelector";
    }

    @Override
    public JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
        ByteBuffer metadata = HAMonitoringProtocol.serializeMetadata(this.identity);

        return new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                Collections.singletonList(new JoinGroupRequestData.JoinGroupRequestProtocol()
                        .setName("HAMonitoringCoordinator")
                        .setMetadata(metadata.array())).iterator());
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        // do nothing - no clean up required
        // xinfra monitor should keep running unless leader has changed
        return;
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(
            String kafkaLeaderId,
            String protocol,
            List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata
    ) {
        Map<String, String> memberIds = new HashMap<>();

        Map<String, HAMonitoringIdentity> assignments = new HashMap<>();
        for (JoinGroupResponseData.JoinGroupResponseMember entry : allMemberMetadata) {
            HAMonitoringIdentity id = HAMonitoringProtocol.deserializeMetadata(ByteBuffer.wrap(entry.metadata()));
            id.setLeader(false);
            memberIds.put(entry.memberId(), id.getId());
            assignments.put(entry.memberId(), id);
        }

        String leaderGroupId = null;
        String leaderId = null;
        // Make member with lexicographically smallest id the leader for kafka monitor
        for (Map.Entry<String, String> memberId : memberIds.entrySet()) {
            if (leaderId == null || memberId.getValue().compareTo(leaderId) < 0) {
                leaderGroupId = memberId.getKey();
                leaderId = memberId.getValue();
            }
        }

        assignments.get(leaderGroupId).setLeader(true);

        Map<String, ByteBuffer> serializedAssignments = new HashMap<>();
        for (Map.Entry<String, HAMonitoringIdentity> entry : assignments.entrySet()) {
            serializedAssignments.put(entry.getKey(), HAMonitoringProtocol.serializeMetadata(entry.getValue()));
        }

        return serializedAssignments;
    }

    @Override
    protected void onJoinComplete(
            int generation,
            String memberId,
            String protocol,
            ByteBuffer memberAssignment
    ) {
        HAMonitoringIdentity assignment = HAMonitoringProtocol.deserializeMetadata(memberAssignment);

        if (assignment.isLeader()) {
            LOG.info("HAMonitoringCoordinator received assignment: is leader");
            try {
                startMonitor.run();
            } catch (Exception e) {
                LOG.error("Error starting HAXinfraMonitor", e);
                // leave group so another member can be elected leader to run the monitor
                maybeLeaveGroup("Failed to start HAXinfraMonitor");
            }
        } else {
            LOG.info("HAMonitoringCoordinator received assignment: is not leader");
            stopMonitor.run();
        }
    }

    public void poll() {
        if (coordinatorUnknown()) {
            ensureCoordinatorReady(time.timer(1000));
        }

        if (rejoinNeededOrPending()) {
            ensureActiveGroup();
        }

        pollHeartbeat(time.milliseconds());
        client.poll(time.timer(1000));
    }
}
