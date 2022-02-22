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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Arrays;

public class StringDimensionIndexer extends MultiValueDictionaryDimensionIndexer<String>
{
  @Nullable
  private static String emptyToNullIfNeeded(@Nullable Object o)
  {
    return o != null ? NullHandling.emptyToNullIfNeeded(o.toString()) : null;
  }

  public StringDimensionIndexer(
      MultiValueHandling multiValueHandling,
      boolean hasBitmapIndexes,
      boolean hasSpatialIndexes,
      boolean useMaxMemoryEstimates
  )
  {
    super(
        new StringDimensionDictionary(!useMaxMemoryEstimates),
        multiValueHandling,
        hasBitmapIndexes,
        hasSpatialIndexes,
        useMaxMemoryEstimates,
        ColumnType.STRING
    );
  }

  /**
   * Estimates size of the given key component.
   * <p>
   * Deprecated method. Use {@link #processRowValsToUnsortedEncodedKeyComponent(Object, boolean)}
   * and {@link EncodedKeyComponent#getEffectiveSizeBytes()}.
   */
  @Override
  public long estimateEncodedKeyComponentSize(int[] key)
  {
    // string length is being accounted for each time they are referenced, based on dimension handler interface,
    // even though they are stored just once. It may overestimate the size by a bit, but we wanted to leave
    // more buffer to be safe
    long estimatedSize = key.length * Integer.BYTES;
    for (int element : key) {
      String val = dimLookup.getValue(element);
      if (val != null) {
        // According to https://www.ibm.com/developerworks/java/library/j-codetoheap/index.html
        // String has the following memory usuage...
        // 28 bytes of data for String metadata (class pointer, flags, locks, hash, count, offset, reference to char array)
        // 16 bytes of data for the char array metadata (class pointer, flags, locks, size)
        // 2 bytes for every letter of the string
        int sizeOfString = 28 + 16 + (2 * val.length());
        estimatedSize += sizeOfString;
      }
    }
    return estimatedSize;
  }

  @Nullable
  @Override
  public Object convertUnsortedEncodedKeyComponentToActualList(int[] key)
  {
    if (key == null || key.length == 0) {
      return null;
    }
    if (key.length == 1) {
      return getActualValue(key[0], false);
    } else {
      String[] rowArray = new String[key.length];
      for (int i = 0; i < key.length; i++) {
        String val = getActualValue(key[i], false);
        rowArray[i] = NullHandling.nullToEmptyIfNeeded(val);
      }
      return Arrays.asList(rowArray);
    }
  }

  @Override
  protected Class<String> getValueClass()
  {
    return String.class;
  }

  @Nullable
  @Override
  protected String parseAndReplaceZeroOrEquivalentToNullIfNeeded(@Nullable Object value)
  {
    return emptyToNullIfNeeded(value);
  }

  @Nullable
  @Override
  protected String parseStringValue(@Nullable String value)
  {
    return value;
  }
}
