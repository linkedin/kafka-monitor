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

/**
 * A base consumer used to abstract different consumer classes
 */
public interface KMBaseConsumer {

  /**
   * The implementation should not block for I/O or have a very short poll time so that the calling thread can attempt
   * to shutdown when the service is stopped.
   * @return  This may return null if the underlying consumer implementation has not returned any messages.
   * @throws Exception
   */
  BaseConsumerRecord receive() throws Exception;

  void close();

}