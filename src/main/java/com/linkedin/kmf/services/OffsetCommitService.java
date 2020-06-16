package com.linkedin.kmf.services;

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;


/**
 *
 */
public class OffsetCommitService implements Service {

  /**
   * The start logic must only execute once.  If an error occurs then the implementer of this class must assume that
   * stop() will be called to clean up.  This method must be thread safe and must assume that stop() may be called
   * concurrently. This can happen if the monitoring application's life cycle is being managed by a container.  Start
   * will only be called once.
   */
  @Override
  public void start() {
    ConsumerNetworkClient consumerNetworkClient =
        new ConsumerNetworkClient(new LogContext(), null, null, new SystemTime(), 3, 3, 3);
    consumerNetworkClient.send(new Node(1, "", 3), null);
  }

  /**
   * This may be called multiple times.  This method must be thread safe and must assume that start() may be called
   * concurrently.  This can happen if the monitoring application's life cycle is being managed by a container.
   * Implementations must be non-blocking and should release the resources acquired by the service during start().
   */
  @Override
  public void stop() {

  }

  /**
   * Implementations of this method must be thread safe as it can be called at any time.  Implementations must be
   * non-blocking.
   * @return true if this start() has returned successfully else this must return false.  This must also return false if
   * the service can no longer perform its function.
   */
  @Override
  public boolean isRunning() {
    return false;
  }

  /**
   * Implementations of this method must be thread safe and must be blocking.
   */
  @Override
  public void awaitShutdown() {

  }
}
