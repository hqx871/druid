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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DummyResourceGroupScheduler implements ResourceGroupScheduler
{
  private final int capacity;

  public DummyResourceGroupScheduler(int capacity)
  {
    this.capacity = capacity;
  }

  @Override
  public int getGroupAvailableCapacity(String resourceGroup)
  {
    return capacity;
  }

  @Override
  public int getGroupCapacity(String resourceGroup)
  {
    return capacity;
  }

  @Override
  public int getTotalCapacity()
  {
    return capacity;
  }

  @Override
  public Set<String> getAllGroup()
  {
    return Collections.emptySet();
  }

  @Override
  public void accquire(String resourceGroup, int permits)
  {
  }

  @Override
  public boolean tryAccquire(String resourceGroup, int permits)
  {
    return true;
  }

  @Override
  public boolean tryAccquire(String resourceGroup, int permits, long timeout, TimeUnit unit)
  {
    return true;
  }

  @Override
  public void release(String resourceGroup, int permits)
  {
  }
}
