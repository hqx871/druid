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

import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

public class LongDictionaryEncodedDimensionHandler extends DictionaryEncodedDimensionHandler<Long>
{
  public LongDictionaryEncodedDimensionHandler(
      String dimensionName,
      MultiValueHandling multiValueHandling,
      boolean hasBitmapIndexes
  )
  {
    super(dimensionName, multiValueHandling, hasBitmapIndexes, false);
  }

  @Override
  public DimensionIndexer<Integer, int[], Long> makeIndexer(boolean useMaxMemoryEstimates)
  {
    return new LongDictionaryEncodedDimensionIndexer(
        multiValueHandling,
        hasBitmapIndexes,
        hasSpatialIndexes,
        useMaxMemoryEstimates
    );
  }

  @Override
  public DimensionMergerV9 makeMerger(
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress,
      Closer closer
  )
  {
    // Sanity-check capabilities.
    if (hasBitmapIndexes != capabilities.hasBitmapIndexes()) {
      throw new ISE(
          "capabilities.hasBitmapIndexes[%s] != this.hasBitmapIndexes[%s]",
          capabilities.hasBitmapIndexes(),
          hasBitmapIndexes
      );
    }

    return new LongDictionaryEncodedDimensionMergerV9(
        dimensionName,
        indexSpec,
        segmentWriteOutMedium,
        capabilities,
        progress,
        closer
    );
  }
}
