/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services;

import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * Factory that constructs the ConsumeService.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ConsumeServiceFactory implements ServiceFactory {
  private final Map _props;
  private final String _name;

  public ConsumeServiceFactory(Map props, String name) {
    _props = props;
    _name = name;
  }

  @Override
  public Service createService() throws Exception {

    CompletableFuture<Void> topicPartitionResult = new CompletableFuture<>();
    topicPartitionResult.complete(null);
    ConsumerFactoryImpl consumerFactory = new ConsumerFactoryImpl(_props);

    return new ConsumeService(_name, topicPartitionResult, consumerFactory);
  }
}
