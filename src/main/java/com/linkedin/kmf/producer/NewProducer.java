/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/*
 * Wrap around the new producer from Apache Kafka and implement the #KMBaseProducer interface
 */
public class NewProducer implements KMBaseProducer {

  private final KafkaProducer<String, String> _producer;

  public NewProducer(Properties producerProps) {
    _producer = new KafkaProducer<>(producerProps);
  }

  @Override
  public RecordMetadata send(BaseProducerRecord baseRecord, boolean sync) throws Exception {
    ProducerRecord<String, String> record =
      new ProducerRecord<>(baseRecord.topic(), baseRecord.partition(), baseRecord.key(), baseRecord.value());
    Future<RecordMetadata> future = _producer.send(record);
    return sync ? future.get() : null;
  }

  @Override
  public void close() {
    _producer.close();
  }

}
