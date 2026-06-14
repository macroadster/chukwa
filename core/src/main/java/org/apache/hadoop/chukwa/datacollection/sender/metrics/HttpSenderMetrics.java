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
package org.apache.hadoop.chukwa.datacollection.sender.metrics;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.ObjectName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HttpSenderMetrics implements HttpSenderActivityMBean {
  private static final Log LOG = LogFactory.getLog(HttpSenderMetrics.class);

  private final AtomicInteger collectorRollover = new AtomicInteger(0);
  private final AtomicInteger httpPost = new AtomicInteger(0);
  private final AtomicInteger httpException = new AtomicInteger(0);
  private final AtomicInteger httpThrowable = new AtomicInteger(0);
  private final AtomicInteger httpTimeOutException = new AtomicInteger(0);
  private ObjectName mbeanName;

  public HttpSenderMetrics(String processName, String recordName) {
    try {
      mbeanName = new ObjectName("chukwa:type=HttpSenderActivity,name=" + recordName);
      ManagementFactory.getPlatformMBeanServer().registerMBean(this, mbeanName);
    } catch (Exception e) {
      LOG.warn("Failed to register HttpSenderMetrics MBean", e);
    }
  }

  @Override
  public int getCollectorRollover() { return collectorRollover.get(); }
  @Override
  public int getHttpPost() { return httpPost.get(); }
  @Override
  public int getHttpException() { return httpException.get(); }
  @Override
  public int getHttpThrowable() { return httpThrowable.get(); }
  @Override
  public int getHttpTimeOutException() { return httpTimeOutException.get(); }

  public void incCollectorRollover() { collectorRollover.incrementAndGet(); }
  public void incHttpPost() { httpPost.incrementAndGet(); }
  public void incHttpException() { httpException.incrementAndGet(); }
  public void incHttpThrowable() { httpThrowable.incrementAndGet(); }
  public void incHttpTimeOutException() { httpTimeOutException.incrementAndGet(); }

  public void shutdown() {
    try {
      if (mbeanName != null) {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(mbeanName);
      }
    } catch (Exception e) {
      LOG.warn("Failed to unregister HttpSenderMetrics MBean", e);
    }
  }
}
