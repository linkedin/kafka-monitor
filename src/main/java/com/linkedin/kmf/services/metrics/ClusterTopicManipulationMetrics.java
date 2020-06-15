/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services.metrics;

import java.util.Map;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;


/**
 * TODO -- in progress!
 */
public class ClusterTopicManipulationMetrics {
  public Metrics _metrics;
  private Sensor _recordsProduced;
  private final Map<String, String> _tags;

  public ClusterTopicManipulationMetrics(final Metrics metrics, final Map<String, String> tags) {
    _metrics = metrics;
    _tags = tags;
  }
}
