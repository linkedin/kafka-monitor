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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/*
 * Wrap around the new consumer from Apache Kafka and implement the #BaseConsumer interface
 */
public class NewConsumer implements BaseConsumer {

  private final KafkaConsumer<String, String> consumer;
  private Iterator<ConsumerRecord<String, String>> recordIter;

  public NewConsumer(String topic, Properties consumerProperties) {
    consumer = new KafkaConsumer<>(consumerProperties);
    consumer.subscribe(Arrays.asList(topic));
    consumer.poll(0);
  }

  @Override
  public BaseConsumerRecord receive() throws Exception {
    if ((recordIter == null || !recordIter.hasNext()))
      recordIter = consumer.poll(60000).iterator();

    if (!recordIter.hasNext())
      return null;

    ConsumerRecord<String, String> record = recordIter.next();
    return new BaseConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), record.value());
  }

  @Override
  public void close() {
    consumer.close();
  }

}
