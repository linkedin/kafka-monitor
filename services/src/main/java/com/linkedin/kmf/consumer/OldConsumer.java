/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/*
 * Wrap around the old consumer from Apache Kafka and implement the #KMBaseConsumer interface
 */
public class OldConsumer implements KMBaseConsumer {

  private final ConsumerConnector _connector;
  private final ConsumerIterator<String, String> _iter;

  public OldConsumer(String topic, Properties consumerProperties) {
    _connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, 1);
    Map<String, List<KafkaStream<String, String>>> kafkaStreams = _connector.createMessageStreams(topicCountMap, new StringDecoder(null), new StringDecoder(null));
    _iter = kafkaStreams.get(topic).get(0).iterator();
  }

  @Override
  public BaseConsumerRecord receive() {
    if (!_iter.hasNext())
      return null;
    MessageAndMetadata<String, String> record = _iter.next();
    return new BaseConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), record.message());
  }

  @Override
  public void close() {
    _connector.shutdown();
  }

}
