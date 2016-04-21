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
import kafka.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import java.util.Properties;

/*
 * Wrap around the old consumer from Apache Kafka and implement the #BaseConsumer interface
 */
public class OldConsumer implements BaseConsumer {

  private final ConsumerConnector _connector;
  private final KafkaStream<String, String> _stream;
  private final ConsumerIterator<String, String> _iter;

  public OldConsumer(String topic, Properties consumerProperties) {
    _connector = Consumer.create(new ConsumerConfig(consumerProperties));
    _stream = _connector.createMessageStreamsByFilter(new Whitelist(topic), 1, new StringDecoder(null), new StringDecoder(null)).head();
    _iter = _stream.iterator();
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
