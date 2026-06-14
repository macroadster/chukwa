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
import javax.management.ObjectName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AgentMetrics implements AgentActivityMBean {
  private static final Log LOG = LogFactory.getLog(AgentMetrics.class);
  public static final AgentMetrics agentMetrics = new AgentMetrics("chukwaAgent", "metrics");

  private final AtomicInteger adaptorCount = new AtomicInteger(0);
  private final AtomicInteger addedAdaptor = new AtomicInteger(0);
  private final AtomicInteger removedAdaptor = new AtomicInteger(0);
  private ObjectName mbeanName;

  public AgentMetrics(String processName, String recordName) {
    try {
      mbeanName = new ObjectName("chukwa:type=AgentActivity,name=" + recordName);
      ManagementFactory.getPlatformMBeanServer().registerMBean(this, mbeanName);
    } catch (Exception e) {
      LOG.warn("Failed to register AgentMetrics MBean", e);
    }
  }

  @Override
  public int getAdaptorCount() { return adaptorCount.get(); }
  @Override
  public int getAddedAdaptor() { return addedAdaptor.get(); }
  @Override
  public int getRemovedAdaptor() { return removedAdaptor.get(); }

  public void setAdaptorCount(int value) { adaptorCount.set(value); }
  public void incAddedAdaptor() { addedAdaptor.incrementAndGet(); }
  public void incRemovedAdaptor() { removedAdaptor.incrementAndGet(); }

  public void shutdown() {
    try {
      if (mbeanName != null) {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(mbeanName);
      }
    } catch (Exception e) {
      LOG.warn("Failed to unregister AgentMetrics MBean", e);
    }
  }
}
