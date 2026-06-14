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
package org.apache.hadoop.chukwa.datacollection.agent.metrics;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.ObjectName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ChunkQueueMetrics implements ChunkQueueActivityMBean {
  private static final Log LOG = LogFactory.getLog(ChunkQueueMetrics.class);

  private final AtomicInteger queueSize = new AtomicInteger(0);
  private final AtomicLong dataSize = new AtomicLong(0);
  private final AtomicInteger addedChunk = new AtomicInteger(0);
  private final AtomicInteger removedChunk = new AtomicInteger(0);
  private final AtomicInteger fullQueue = new AtomicInteger(0);
  private ObjectName mbeanName;

  public ChunkQueueMetrics(String processName, String recordName) {
    try {
      mbeanName = new ObjectName("chukwa:type=QueueActivity,name=" + recordName);
      ManagementFactory.getPlatformMBeanServer().registerMBean(this, mbeanName);
    } catch (Exception e) {
      LOG.warn("Failed to register ChunkQueueMetrics MBean", e);
    }
  }

  @Override
  public int getQueueSize() { return queueSize.get(); }
  @Override
  public long getDataSize() { return dataSize.get(); }
  @Override
  public int getAddedChunk() { return addedChunk.get(); }
  @Override
  public int getRemovedChunk() { return removedChunk.get(); }
  @Override
  public int getFullQueue() { return fullQueue.get(); }

  public void setQueueSize(int value) { queueSize.set(value); }
  public void setDataSize(long value) { dataSize.set(value); }
  public void incAddedChunk() { addedChunk.incrementAndGet(); }
  public void incRemovedChunk() { removedChunk.incrementAndGet(); }
  public void setFullQueue(int value) { fullQueue.set(value); }

  public void shutdown() {
    try {
      if (mbeanName != null) {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(mbeanName);
      }
    } catch (Exception e) {
      LOG.warn("Failed to unregister ChunkQueueMetrics MBean", e);
    }
  }
}
