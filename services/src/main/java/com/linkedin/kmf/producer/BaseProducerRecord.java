/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.producer;

public class BaseProducerRecord {
  private final String topic;
  private final Integer partition;
  private final String key;
  private final String value;

  public BaseProducerRecord(String topic, Integer partition, String key, String value) {
    this.topic = topic;
    this.partition = partition;
    this.key = key;
    this.value = value;
  }

  public String topic() {
    return topic;
  }

  public Integer partition() {
    return partition;
  }

  public String key() {
    return key;
  }

  public String value() {
    return value;
  }

  @Override
  public String toString() {
    return "record(topic:" + topic + ",partition:" + partition + ",key:" + key + ",value:" + value + ")";
  }

}
