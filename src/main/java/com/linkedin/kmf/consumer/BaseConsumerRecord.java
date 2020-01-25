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

public class BaseConsumerRecord {

  private final String _topic;
  private final int _partition;
  private final long _offset;
  private final String _key;
  private final String _value;

  public BaseConsumerRecord(String topic, int partition, long offset, String key, String value) {
    _topic = topic;
    _partition = partition;
    _offset = offset;
    _key = key;
    _value = value;
  }

  public String topic() {
    return _topic;
  }

  public int partition() {
    return _partition;
  }

  public long offset() {
    return _offset;
  }

  public String key() {
    return _key;
  }

  public String value() {
    return _value;
  }

  @Override
  public String toString() {
    return "record(topic:" + _topic + ",partition:" + _partition + ",offset:" + _offset + ",key:" + _key + ",value:" + _value + ")";
  }

}
