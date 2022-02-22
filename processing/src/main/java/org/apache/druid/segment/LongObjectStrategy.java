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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;

public class LongObjectStrategy implements ObjectStrategy<Long>
{
  public static final LongObjectStrategy INSTANCE = new LongObjectStrategy();

  private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

  private static final Comparator<Long> COMPARATOR = Comparators.<Long>naturalNullsFirst();

  @Override
  public Class<? extends Long> getClazz()
  {
    return Long.class;
  }

  @Nullable
  @Override
  public Long fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    if (numBytes == 0 || buffer == null) {
      return null;
    }
    byte[] bytes = new byte[Long.BYTES];
    buffer.get(bytes);
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.order(BYTE_ORDER);
    return byteBuffer.getLong();
  }

  @Nullable
  @Override
  public byte[] toBytes(@Nullable Long val)
  {
    if (val == null) {
      return null;
    }
    byte[] bytes = new byte[Long.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.order(BYTE_ORDER);
    byteBuffer.putLong(val);
    return bytes;
  }

  @Override
  public int compare(Long o1, Long o2)
  {
    return COMPARATOR.compare(o1, o2);
  }
}
