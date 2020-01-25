/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.partitioner;

public class NewKMPartitioner implements KMPartitioner {

  public int partition(String key, int partitionNum) {
    byte[] keyBytes = key.getBytes();
    return toPositive(org.apache.kafka.common.utils.Utils.murmur2(keyBytes)) % partitionNum;
  }

  private static int toPositive(int number) {
    return number & 0x7fffffff;
  }

}
