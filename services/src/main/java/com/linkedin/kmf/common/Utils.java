/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.common;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooKeeper;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  /**
   * Read number of partitions for the given topic on the specified zookeeper
   * @param zkUrl zookeeper connection url
   * @param topic topic name
   *
   * @return the number of partitions of the given topic
   */
  public static int getPartitionNumForTopic(String zkUrl, String topic) {
    try {
      ZooKeeper zk = new ZooKeeper(zkUrl, 3000, null);
      String path = "/brokers/topics/" + topic;
      String data = new String(zk.getData(path, false, new Stat()));
      zk.close();
      int partitionNum = (new JSONObject(data)).getJSONObject("partitions").keySet().size();
      return partitionNum;
    } catch (Exception e) {
      LOG.error("Error when getting data from zookeeper", e);
      return -1;
    }
  }

  /**
   * Read a properties file from the given path
   * @param filename The path of the file to read
   */
  public static Properties loadProps(String filename, Properties defaults) throws IOException {
    Properties props = defaults != null ? defaults : new Properties();
    try (InputStream propStream = new FileInputStream(filename)) {
      props.load(propStream);
    }

    return props;
  }

  /**
   * @param timestamp time in Ms when this message is generated
   * @param topic     topic this message is sent to
   * @param idx       index is consecutive numbers used by KafkaMonitor to determine duplicate or lost messages
   * @param msgSize   size of the message
   * @return          string that encodes the above fields
   */
  public static String jsonFromFields(String topic, long idx, long timestamp, String producerId, int msgSize) {
    GenericRecord record = new GenericData.Record(Protocol.MESSAGE_V0);
    record.put(Protocol.TOPIC_FIELD.name(), topic);
    record.put(Protocol.INDEX_FIELD.name(), idx);
    record.put(Protocol.TIME_FIELD.name(), timestamp);
    record.put(Protocol.PRODUCER_ID_FIELD.name(), producerId);
    // CONTENT_FIELD is composed of #msgSize number of character 'x', e.g. xxxxxxxxxx
    record.put(Protocol.CONTENT_FIELD.name(), String.format("%1$-" + msgSize + "s", "").replace(' ', 'x'));
    return jsonFromGenericRecord(record);
  }

  /**
   * @param message kafka message in the string format
   * @return        GenericRecord that is deserialized from kafka message w.r.t. expected schema
   */
  public static GenericRecord genericRecordFromJson(String message) {
    GenericRecord record = new GenericData.Record(Protocol.MESSAGE_V0);
    JSONObject jsonObject = new JSONObject(message);
    record.put(Protocol.TOPIC_FIELD.name(), jsonObject.getString(Protocol.TOPIC_FIELD.name()));
    record.put(Protocol.INDEX_FIELD.name(), jsonObject.getLong(Protocol.INDEX_FIELD.name()));
    record.put(Protocol.TIME_FIELD.name(), jsonObject.getLong(Protocol.TIME_FIELD.name()));
    record.put(Protocol.PRODUCER_ID_FIELD.name(), jsonObject.getString(Protocol.PRODUCER_ID_FIELD.name()));
    record.put(Protocol.CONTENT_FIELD.name(), jsonObject.getString(Protocol.CONTENT_FIELD.name()));
    return record;
  }

  public static String jsonFromGenericRecord(GenericRecord record) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(Protocol.MESSAGE_V0);

    try {
      Encoder encoder = new JsonEncoder(Protocol.MESSAGE_V0, out);
      writer.write(record, encoder);
      encoder.flush();
    }
    catch (IOException e){
      LOG.error("Unable to serialize avro record due to error " + e);
    }
    return out.toString();
  }

  /**
   * Override entry for key with value in properties if value is not an empty string
   */
  public static void overrideIfDefined(Properties properties, String key, String value) {
    if (value != null && value.length() > 0)
      properties.put(key, value);
  }

}
