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

package org.apache.druid.segment.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.column.DictionaryWrapStrategy;

import java.util.Objects;

public class DictionarySparseIndexWrapStrategy<T> implements DictionaryWrapStrategy<T>
{
  private final int rowThreshold;
  private final int indexLimit;
  private final int defaultGranularity;

  @JsonCreator
  public DictionarySparseIndexWrapStrategy(
      @JsonProperty("defaultGranularity") int defaultGranularity,
      @JsonProperty("rowThreshold") int rowThreshold,
      @JsonProperty("indexLimit") int indexLimit
  )
  {
    this.defaultGranularity = defaultGranularity > 0 ? defaultGranularity : 1024;
    this.rowThreshold = rowThreshold > 0 ? rowThreshold : 64 << 10;
    this.indexLimit = indexLimit > 0 ? indexLimit : 512;
  }

  @Override
  public Indexed<T> wrap(GenericIndexed<T> indexed)
  {
    if (indexed.size() >= rowThreshold) {
      int indexGranularity = Math.min(defaultGranularity, indexed.size() / indexLimit);
      return new SparseArrayIndexed<>(indexed, indexGranularity);
    }
    return indexed;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DictionarySparseIndexWrapStrategy<?> that = (DictionarySparseIndexWrapStrategy<?>) o;
    return rowThreshold == that.rowThreshold
           && indexLimit == that.indexLimit
           && defaultGranularity == that.defaultGranularity;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rowThreshold, indexLimit, defaultGranularity);
  }

  @Override
  public String toString()
  {
    return "DictionarySparseArrayIndexWrapper{" +
           "rowThreshold=" + rowThreshold +
           ", indexLimit=" + indexLimit +
           ", defaultGranularity=" + defaultGranularity +
           '}';
  }
}
