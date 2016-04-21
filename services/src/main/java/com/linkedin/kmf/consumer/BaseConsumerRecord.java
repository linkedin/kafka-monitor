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

public class BaseConsumerRecord {

  private final String topic;
  private final int partition;
  private final long offset;
  private final String key;
  private final String value;

  public BaseConsumerRecord(String topic, int partition, long offset, String key, String value) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.key = key;
    this.value = value;
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }

  public long offset() {
    return offset;
  }

  public String key() {
    return key;
  }

  public String value() {
    return value;
  }

  @Override
  public String toString() {
    return "record(topic:" + topic + ",partition:" + partition + ",offset:" + offset + ",key:" + key + ",value:" + value + ")";
  }

}
