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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.PeekingIterator;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.serde.LongDictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.util.Collections;
import java.util.Comparator;

public class LongDictionaryEncodedDimensionMergerV9 extends DictionaryEncodedColumnMerger<Long>
{
  private static final Indexed<Long> NULL_STR_DIM_VAL = new ListIndexed<>(Collections.singletonList(null));

  @VisibleForTesting
  public static final Comparator<Pair<Integer, PeekingIterator<Long>>> DICTIONARY_MERGING_COMPARATOR =
      DictionaryMergingIterator.makePeekingComparator();

  public LongDictionaryEncodedDimensionMergerV9(
      String dimensionName,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress,
      Closer closer
  )
  {
    super(dimensionName, indexSpec, segmentWriteOutMedium, capabilities, progress, closer);
  }

  @Override
  protected Comparator<Pair<Integer, PeekingIterator<Long>>> getDictionaryMergingComparator()
  {
    return DICTIONARY_MERGING_COMPARATOR;
  }

  @Override
  protected Indexed<Long> getNullDimValue()
  {
    return NULL_STR_DIM_VAL;
  }

  @Override
  protected ObjectStrategy<Long> getObjectStrategy()
  {
    return LongObjectStrategy.INSTANCE;
  }

  @Override
  protected Long coerceValue(Long value)
  {
    return value;
  }

  @Override
  public ColumnDescriptor makeColumnDescriptor()
  {
    // Now write everything
    boolean hasMultiValue = capabilities.hasMultipleValues().isTrue();
    final CompressionStrategy compressionStrategy = indexSpec.getDimensionCompression();
    final BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();

    final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
    builder.setValueType(ValueType.LONG);
    builder.setHasMultipleValues(hasMultiValue);
    final DictionaryEncodedColumnPartSerde.SerializerBuilder partBuilder = LongDictionaryEncodedColumnPartSerde
        .serializerBuilder()
        .withDictionary(dictionaryWriter)
        .withValue(
            encodedValueSerializer,
            hasMultiValue,
            compressionStrategy != CompressionStrategy.UNCOMPRESSED
        )
        .withBitmapSerdeFactory(bitmapSerdeFactory)
        .withBitmapIndex(bitmapWriter)
        .withByteOrder(IndexIO.BYTE_ORDER);

    return builder
        .addSerde(partBuilder.build())
        .build();
  }
}
