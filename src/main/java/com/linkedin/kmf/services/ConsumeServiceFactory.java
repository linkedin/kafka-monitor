package com.linkedin.kmf.services;

import java.util.Map;
import java.util.concurrent.CompletableFuture;


@SuppressWarnings("rawtypes")
public class ConsumeServiceFactory implements ServiceFactory {
  private final Map _props;
  private final String _name;

  public ConsumeServiceFactory(Map props, String name) {
    _props = props;
    _name = name;
  }

  @Override
  public Service create() throws Exception {
//    Constructor<?>[] constructors = Class.forName(className).getConstructors();

    CompletableFuture<Void> topicPartitionResult = new CompletableFuture<>();
    topicPartitionResult.complete(null);
    ConsumerFactoryImpl consumerFactory = new ConsumerFactoryImpl(_props);
    return new ConsumeService("", topicPartitionResult, consumerFactory);
  }
}
