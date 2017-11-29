/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.storage.partitioner;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class MultiFieldPartitioner<T> extends TimeBasedPartitioner<T> {

  private static final Logger log = LoggerFactory.getLogger(MultiFieldPartitioner.class);
//  private static String timeField;
//  private static String timeFormat;
  private FieldPartitioner<T> fieldPartitioner = new FieldPartitioner<>();
  private TimestampExtractor backupExtractor;
//  private SimpleDateFormat mDtFormatter;
//  private String[] handledDateFormats = {"yyyMMdd", "yyyy-MM-dd'T'HH:mm:ss", "epoch_secs"};

  @Override
  public void configure(Map<String, Object> config) {
    fieldPartitioner.configure(config);
    try {
      config.put(PartitionerConfig.SCHEMA_GENERATOR_CLASS_CONFIG, Class.forName("io.confluent.connect.storage.hive.schema.TimeBasedSchemaGenerator"));
    } catch (ClassNotFoundException e) {
      log.error("Class not found ", e);
    }
    if (config.get(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG) != null && config.get(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG).equals("Record")) {
      if (config.containsKey(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG)) {
        backupExtractor = newTimestampExtractor("RecordField");
        backupExtractor.configure(config);
      }
    }

    super.configure(config);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    log.info("MULTI FIELD PARITIONER ENCODING PARITION");
    String timePartition;
    try {
      timePartition = super.encodePartition(sinkRecord);
    } catch (Exception e) {
      log.warn("Couldn't extract time stamp trying back up");
      timestampExtractor = backupExtractor;
      timePartition = super.encodePartition(sinkRecord);
    }
    String fieldPartition = fieldPartitioner.encodePartition(sinkRecord);
    return "dt=" + timePartition + delim + fieldPartition;
  }

}
