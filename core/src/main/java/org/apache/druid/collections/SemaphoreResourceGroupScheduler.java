/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.collections;

import com.google.common.base.Strings;
import org.apache.druid.java.util.common.IAE;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class SemaphoreResourceGroupScheduler implements ResourceGroupScheduler
{
  private final int capacity;
  private final Map<String, Integer> groupPermitMap;
  private final Map<String, Semaphore> groupRegistry;

  public SemaphoreResourceGroupScheduler(int capacity, Map<String, Integer> config)
  {
    this.capacity = capacity;
    this.groupPermitMap = config;
    this.groupRegistry = new HashMap<>();
    groupPermitMap.forEach((group, permits) -> {
      Semaphore semaphore = new Semaphore(permits, true);
      groupRegistry.put(group, semaphore);
    });
  }

  @Override
  public int getGroupAvailableCapacity(String resourceGroup)
  {
    if (isDefaultGroup(resourceGroup)) {
      return capacity;
    } else {
      return getGroupSemaphore(resourceGroup).availablePermits();
    }
  }

  @Override
  public int getGroupCapacity(String resourceGroup)
  {
    if (isDefaultGroup(resourceGroup)) {
      return capacity;
    } else {
      return groupPermitMap.getOrDefault(resourceGroup, -1);
    }
  }

  @Override
  public int getTotalCapacity()
  {
    return capacity;
  }

  @Override
  public Set<String> getAllGroup()
  {
    return groupPermitMap.keySet();
  }

  @Override
  public void accquire(String resourceGroup, int permits) throws InterruptedException
  {
    if (!isDefaultGroup(resourceGroup)) {
      getGroupSemaphore(resourceGroup).acquire(permits);
    }
  }

  private Semaphore getGroupSemaphore(String group)
  {
    Semaphore semaphore = groupRegistry.get(group);
    if (semaphore == null) {
      throw new IAE("resource group %s not exists", group);
    }
    return semaphore;
  }

  @Override
  public boolean tryAccquire(String resourceGroup, int permits)
  {
    if (isDefaultGroup(resourceGroup)) {
      return true;
    }
    return getGroupSemaphore(resourceGroup).tryAcquire(permits);
  }

  private boolean isDefaultGroup(String resourceGroup)
  {
    return Strings.isNullOrEmpty(resourceGroup);
  }

  @Override
  public boolean tryAccquire(String resourceGroup, int permits, long timeout, TimeUnit unit) throws InterruptedException
  {
    if (isDefaultGroup(resourceGroup)) {
      return true;
    }
    return getGroupSemaphore(resourceGroup).tryAcquire(permits, timeout, unit);
  }

  @Override
  public void release(String resourceGroup, int permits)
  {
    if (!isDefaultGroup(resourceGroup)) {
      getGroupSemaphore(resourceGroup).release(permits);
    }
  }
}
