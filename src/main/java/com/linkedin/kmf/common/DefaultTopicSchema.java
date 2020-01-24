/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.common;

import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

public class DefaultTopicSchema {

  static final Field TOPIC_FIELD = new Field("topic", Schema.create(Schema.Type.STRING), null, null);

  public static final Field TIME_FIELD = new Field("time", Schema.create(Schema.Type.LONG), null, null);

  public static final Field INDEX_FIELD = new Field("index", Schema.create(Schema.Type.LONG), null, null);

  static final Field PRODUCER_ID_FIELD = new Field("producerId", Schema.create(Schema.Type.STRING), null, null);

  static final Field CONTENT_FIELD = new Field("content", Schema.create(Schema.Type.STRING), null, null);

  static final Schema MESSAGE_V0;

  static {
    MESSAGE_V0 = Schema.createRecord("KafkaMonitorSchema", null, "kafka.monitor", false);
    MESSAGE_V0.setFields(Arrays.asList(TOPIC_FIELD, TIME_FIELD, INDEX_FIELD, PRODUCER_ID_FIELD, CONTENT_FIELD));
  }

}
