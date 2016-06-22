/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.services.configs;

public class CommonServiceConfig {

  public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
  public static final String ZOOKEEPER_CONNECT_DOC = "Zookeeper connect string.";

  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  public static final String BOOTSTRAP_SERVERS_DOC = "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form "
    + "<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to "
    + "discover the full cluster membership (which may change dynamically), this list need not contain the full set of "
    + "servers (you may want more than one, though, in case a server is down).";

  public static final String TOPIC_CONFIG = "topic";
  public static final String TOPIC_DOC = "Topic to be used by the service.";

}