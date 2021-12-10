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

/**
 * Define methods to serialize and desercialize HAMonitoringIdentity instances. This is used by the coordinator to
 * perform assignments and group joins.
 */
class HAMonitoringProtocol {
    public static ByteBuffer serializeMetadata(HAMonitoringIdentity id) {
        return ByteBuffer.wrap(id.toBytes());
    }

    public static HAMonitoringIdentity deserializeMetadata(ByteBuffer data) {
        return HAMonitoringIdentity.fromBytes(data.array());
    }
}

/**
 * This coordinator, based on Kafka's AbstractCooridnator, manages the instances of Xinfra Monitor running in the
 * group specified in the config. One of these instances is elected to report metrics. That instance, referred to as
 * the leader, will run Xinfra Monitor to report metrics. If the leader fails or leaves the group, a different instance
 * will start running Xinfra Monitor.
 */
public class HAMonitoringCoordinator extends AbstractCoordinator {
    private final Runnable _startMonitor;
    private final Runnable _stopMonitor;
    private HAMonitoringIdentity _identity;
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

        this._startMonitor = start;
        this._stopMonitor = stop;
        this._identity = id;
        this.LOG = logContext.logger(HAMonitoringCoordinator.class);
    }

    @Override
    public String protocolType() {
        return "xinfraleaderelector";
    }

    @Override
    public JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
        ByteBuffer metadata = HAMonitoringProtocol.serializeMetadata(this._identity);

        return new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                Collections.singletonList(new JoinGroupRequestData.JoinGroupRequestProtocol()
                        .setName("HAMonitoringCoordinator")
                        .setMetadata(metadata.array())).iterator());
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        /**
         * When a new member joins the group, do nothing - no clean up required. Xinfra Monitor should keep running
         * until the leader has changed.
         */
    }

    /**
     * One group member will perform assignemnt for the group. This method determines which member should report
     * metrics and returns that assignment to all members.
     *
     * Unless a leader already exists, the member with the lexicographically smallest group ID is chosen as the leader.
     * The group ID is used instead of the user defined ID since the latter is not guaranteed to be unique.
     */
    @Override
    protected Map<String, ByteBuffer> performAssignment(
            String leaderId,
            String protocol,
            List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata
    ) {
        // Map the group defined member ID to HAMonitoringIdentity object
        Map<String, HAMonitoringIdentity> assignments = new HashMap<>();
        int numLeaders = 0;
        String leaderGroupId = null; // Store the leader's group defined member ID

        for (JoinGroupResponseData.JoinGroupResponseMember entry : allMemberMetadata) {
            HAMonitoringIdentity id = HAMonitoringProtocol.deserializeMetadata(ByteBuffer.wrap(entry.metadata()));
            if (id.isLeader()) numLeaders++;

            // Update lexicographically smallest group defined id
            if (leaderGroupId == null || entry.memberId().compareTo(leaderGroupId) < 0) {
                leaderGroupId = entry.memberId();
            }

            assignments.put(entry.memberId(), id);
        }

        if (numLeaders != 1) {
            // Make member with lexicographically smallest group id the leader to run Xinfra Monitor
            for (Map.Entry<String, HAMonitoringIdentity> entry : assignments.entrySet()) {
                entry.getValue().setLeader(entry.getKey() == leaderGroupId);
            }
        } // Otherwise, leave the current leader

        // Map group defined id to serialized identity object
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
        /**
         * Only the assigned leader should run Xinfra Monitor. All other members should stop running the monitor.
         * The start and stop methods are defined in Xinfra Monitor's constructor.
         */
        this._identity = HAMonitoringProtocol.deserializeMetadata(memberAssignment);

        if (this._identity.isLeader()) {
            LOG.info("HAMonitoringCoordinator received assignment: is leader");
            try {
                _startMonitor.run();
            } catch (Exception e) {
                LOG.error("Error starting HAXinfraMonitor", e);
                // Leave group so another member can be elected leader to run the monitor
                maybeLeaveGroup("Failed to start HAXinfraMonitor");
            }
        } else {
            LOG.info("HAMonitoringCoordinator received assignment: is not leader");
            _stopMonitor.run();
        }
    }

    /**
     * The service will poll the coordinator at a fixed rate. If a reassignment is required, the coordinator will handle
     * that.
     */
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
