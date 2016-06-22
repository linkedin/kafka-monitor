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

import java.util.Properties;

/*
 * Wrap around the new consumer from Apache Kafka and implement the #KMBaseConsumer interface
 */
public class NewConsumer implements KMBaseConsumer {

  public NewConsumer(String topic, Properties consumerProperties) throws Exception {
    throw new Exception("New consumer is not supported in kafka-0.8.2.2");
  }

  @Override
  public BaseConsumerRecord receive() throws Exception {
    return null;
  }

  @Override
  public void close() {

  }

}
