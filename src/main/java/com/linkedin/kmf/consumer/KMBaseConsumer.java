/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.consumer;

import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;


/**
 * A base consumer used to abstract different consumer classes.
 *
 * Implementations of this class must have constructor with the following signature:
 *   Constructor({@link java.util.Properties} properties).
 */
public interface KMBaseConsumer {

  BaseConsumerRecord receive() throws Exception;

  void commitAsync();

  void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

  void commitAsync(OffsetCommitCallback callback);

  OffsetAndMetadata committed(TopicPartition tp);

  void close();

  long lastCommitted();

  void updateLastCommit();

}
