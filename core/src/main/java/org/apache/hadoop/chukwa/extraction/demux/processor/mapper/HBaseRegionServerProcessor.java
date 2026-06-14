/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

@Tables(annotations = { @Table(name = "HBase", columnFamily = "regionserver") })
public class HBaseRegionServerProcessor extends AbstractProcessor {

  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
      throws Throwable {

    Logger log = Logger.getLogger(HBaseRegionServerProcessor.class);
    long timeStamp = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        .getTimeInMillis();
    ChukwaRecord record = new ChukwaRecord();

    Map<String, ByteBuffer> metricsMap = new HashMap<String, ByteBuffer>();

    try {
      JSONObject obj = (JSONObject) JSONValue.parse(recordEntry);
      String ttTag = chunk.getTag("timeStamp");
      if (ttTag == null) {
        log.warn("timeStamp tag not set in JMX adaptor for hbase region server");
      } else {
        timeStamp = Long.parseLong(ttTag);
      }
      @SuppressWarnings("unchecked")
      Iterator<Map.Entry<String, ?>> keys = obj.entrySet().iterator();

      while (keys.hasNext()) {
        Map.Entry<String, ?> entry = keys.next();
        String key = entry.getKey();
        Object value = entry.getValue();
        String valueString = value == null ? "" : value.toString();
        ByteBuffer b = ByteBuffer.wrap(valueString.getBytes(Charset.forName("UTF-8")));
        metricsMap.put(key, b);
      }

      TreeMap<String, ByteBuffer> t = new TreeMap<String, ByteBuffer>(metricsMap);
      record.setMapFields(t);
      buildGenericRecord(record, null, timeStamp, "regionserver");
      output.collect(key, record);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }
}
