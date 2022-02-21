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

package org.apache.druid.segment.column;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.CachingIndexed;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.historical.HistoricalDimensionSelector;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 *
 */
public abstract class BaseDictionaryEncodedColumn<ValType extends Comparable<ValType>>
    implements DictionaryEncodedColumn<ValType>
{
  @Nullable
  private final ColumnarInts column;
  @Nullable
  private final ColumnarMultiInts multiValueColumn;
  private final CachingIndexed<ValType> cachedDictionary;
  private final Indexed<ByteBuffer> dictionaryUtf8;

  public BaseDictionaryEncodedColumn(
      @Nullable ColumnarInts singleValueColumn,
      @Nullable ColumnarMultiInts multiValueColumn,
      CachingIndexed<ValType> dictionary,
      Indexed<ByteBuffer> dictionaryUtf8
  )
  {
    this.column = singleValueColumn;
    this.multiValueColumn = multiValueColumn;
    this.cachedDictionary = dictionary;
    this.dictionaryUtf8 = dictionaryUtf8;
  }

  @Override
  public final int length()
  {
    return hasMultipleValues() ? multiValueColumn.size() : column.size();
  }

  @Override
  public final boolean hasMultipleValues()
  {
    return column == null;
  }

  @Override
  public final int getSingleValueRow(int rowNum)
  {
    return column.get(rowNum);
  }

  @Override
  public final IndexedInts getMultiValueRow(int rowNum)
  {
    return multiValueColumn.get(rowNum);
  }

  @Override
  @Nullable
  public final ValType lookupName(int id)
  {
    return cachedDictionary.get(id);
  }


  /**
   * Returns the value for a particular dictionary id as UTF-8 bytes.
   * <p>
   * The returned buffer is in big-endian order. It is not reused, so callers may modify the position, limit, byte
   * order, etc of the buffer.
   * <p>
   * The returned buffer points to the original data, so callers must take care not to use it outside the valid
   * lifetime of this column.
   *
   * @param id id to lookup the dictionary value for
   * @return dictionary value for the given id, or null if the value is itself null
   */
  @Nullable
  public final ByteBuffer lookupNameUtf8(int id)
  {
    return dictionaryUtf8.get(id);
  }

  @Override
  public final int lookupId(ValType name)
  {
    return cachedDictionary.indexOf(name);
  }

  @Override
  public final int getCardinality()
  {
    return cachedDictionary.size();
  }

  @Override
  public final HistoricalDimensionSelector makeDimensionSelector(
      final ReadableOffset offset,
      @Nullable final ExtractionFn extractionFn
  )
  {
    abstract class QueryableDimensionSelector extends AbstractDimensionSelector
        implements HistoricalDimensionSelector, IdLookup<ValType>
    {
      @Override
      public int getValueCardinality()
      {
        /*
         This is technically wrong if
         extractionFn != null && (extractionFn.getExtractionType() != ExtractionFn.ExtractionType.ONE_TO_ONE ||
                                    !extractionFn.preservesOrdering())
         However current behavior allows some GroupBy-V1 queries to work that wouldn't work otherwise and doesn't
         cause any problems due to special handling of extractionFn everywhere.
         See https://github.com/apache/druid/pull/8433
         */
        return getCardinality();
      }

      @Override
      public String lookupName(int id)
      {
        final ValType value = BaseDictionaryEncodedColumn.this.lookupName(id);
        return extractionFn == null ? convertToStringName(value) : extractionFn.apply(value);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return BaseDictionaryEncodedColumn.this.lookupNameUtf8(id);
      }

      @Override
      public boolean supportsLookupNameUtf8()
      {
        return true;
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup<ValType> idLookup()
      {
        return extractionFn == null ? this : null;
      }

      @Override
      public int lookupId(ValType name)
      {
        if (extractionFn != null) {
          throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
        }
        return BaseDictionaryEncodedColumn.this.lookupId(name);
      }
    }

    if (hasMultipleValues()) {
      class MultiValueDimensionSelector extends QueryableDimensionSelector
      {
        @Override
        public IndexedInts getRow()
        {
          return multiValueColumn.get(offset.getOffset());
        }

        @Override
        public IndexedInts getRow(int offset)
        {
          return multiValueColumn.get(offset);
        }

        @Override
        public ValueMatcher makeValueMatcher(@Nullable String value)
        {
          return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
        }

        @Override
        public ValueMatcher makeValueMatcher(Predicate<String> predicate)
        {
          return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
        }

        @Nullable
        @Override
        public Object getObject()
        {
          return defaultGetObject();
        }

        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("multiValueColumn", multiValueColumn);
          inspector.visit("offset", offset);
          inspector.visit("extractionFn", extractionFn);
        }
      }
      return new MultiValueDimensionSelector();
    } else {
      class SingleValueQueryableDimensionSelector extends QueryableDimensionSelector
          implements SingleValueHistoricalDimensionSelector
      {
        private final SingleIndexedInt row = new SingleIndexedInt();

        @Override
        public IndexedInts getRow()
        {
          row.setValue(getRowValue());
          return row;
        }

        public int getRowValue()
        {
          return column.get(offset.getOffset());
        }

        @Override
        public IndexedInts getRow(int offset)
        {
          row.setValue(getRowValue(offset));
          return row;
        }

        @Override
        public int getRowValue(int offset)
        {
          return column.get(offset);
        }

        @Override
        public ValueMatcher makeValueMatcher(final @Nullable String value)
        {
          if (extractionFn == null) {
            final int valueId = lookupId(convertFromStringName(value));
            if (valueId >= 0) {
              return new ValueMatcher()
              {
                @Override
                public boolean matches()
                {
                  return getRowValue() == valueId;
                }

                @Override
                public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                {
                  inspector.visit("column", BaseDictionaryEncodedColumn.this);
                }
              };
            } else {
              return BooleanValueMatcher.of(false);
            }
          } else {
            // Employ caching BitSet optimization
            return makeValueMatcher(Predicates.equalTo(value));
          }
        }

        @Override
        public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
        {
          final BitSet checkedIds = new BitSet(getCardinality());
          final BitSet matchingIds = new BitSet(getCardinality());

          // Lazy matcher; only check an id if matches() is called.
          return new ValueMatcher()
          {
            @Override
            public boolean matches()
            {
              final int id = getRowValue();

              if (checkedIds.get(id)) {
                return matchingIds.get(id);
              } else {
                final boolean matches = predicate.apply(lookupName(id));
                checkedIds.set(id);
                if (matches) {
                  matchingIds.set(id);
                }
                return matches;
              }
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
              inspector.visit("column", BaseDictionaryEncodedColumn.this);
            }
          };
        }

        @Override
        public Object getObject()
        {
          return lookupName(getRowValue());
        }

        @Override
        public Class classOfObject()
        {
          return String.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("column", column);
          inspector.visit("offset", offset);
          inspector.visit("extractionFn", extractionFn);
        }
      }
      return new SingleValueQueryableDimensionSelector();
    }
  }

  @Override
  public final SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(final ReadableVectorOffset offset)
  {
    class QueryableSingleValueDimensionVectorSelector implements SingleValueDimensionVectorSelector, IdLookup<ValType>
    {
      private final int[] vector = new int[offset.getMaxVectorSize()];
      private int id = ReadableVectorInspector.NULL_ID;

      @Override
      public int[] getRowVector()
      {
        if (id == offset.getId()) {
          return vector;
        }

        if (offset.isContiguous()) {
          column.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          column.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
        }

        id = offset.getId();
        return vector;
      }

      @Override
      public int getValueCardinality()
      {
        return getCardinality();
      }

      @Nullable
      @Override
      public String lookupName(final int id)
      {
        return convertToStringName(BaseDictionaryEncodedColumn.this.lookupName(id));
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return BaseDictionaryEncodedColumn.this.lookupNameUtf8(id);
      }

      @Override
      public boolean supportsLookupNameUtf8()
      {
        return true;
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup<ValType> idLookup()
      {
        return this;
      }

      @Override
      public int lookupId(@Nullable final ValType name)
      {
        return BaseDictionaryEncodedColumn.this.lookupId(name);
      }

      @Override
      public int getCurrentVectorSize()
      {
        return offset.getCurrentVectorSize();
      }

      @Override
      public int getMaxVectorSize()
      {
        return offset.getMaxVectorSize();
      }
    }

    return new QueryableSingleValueDimensionVectorSelector();
  }

  @Override
  public final MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(final ReadableVectorOffset offset)
  {
    class QueryableMultiValueDimensionVectorSelector implements MultiValueDimensionVectorSelector, IdLookup<ValType>
    {
      private final IndexedInts[] vector = new IndexedInts[offset.getMaxVectorSize()];
      private int id = ReadableVectorInspector.NULL_ID;

      @Override
      public IndexedInts[] getRowVector()
      {
        if (id == offset.getId()) {
          return vector;
        }

        if (offset.isContiguous()) {
          final int currentOffset = offset.getStartOffset();
          final int numRows = offset.getCurrentVectorSize();

          for (int i = 0; i < numRows; i++) {
            // Must use getUnshared, otherwise all elements in the vector could be the same shared object.
            vector[i] = multiValueColumn.getUnshared(i + currentOffset);
          }
        } else {
          final int[] offsets = offset.getOffsets();
          final int numRows = offset.getCurrentVectorSize();

          for (int i = 0; i < numRows; i++) {
            // Must use getUnshared, otherwise all elements in the vector could be the same shared object.
            vector[i] = multiValueColumn.getUnshared(offsets[i]);
          }
        }

        id = offset.getId();
        return vector;
      }

      @Override
      public int getValueCardinality()
      {
        return getCardinality();
      }

      @Nullable
      @Override
      public String lookupName(final int id)
      {
        return convertToStringName(BaseDictionaryEncodedColumn.this.lookupName(id));
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return BaseDictionaryEncodedColumn.this.lookupNameUtf8(id);
      }

      @Override
      public boolean supportsLookupNameUtf8()
      {
        return true;
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup<ValType> idLookup()
      {
        return this;
      }

      @Override
      public int lookupId(@Nullable final ValType name)
      {
        return BaseDictionaryEncodedColumn.this.lookupId(name);
      }

      @Override
      public int getCurrentVectorSize()
      {
        return offset.getCurrentVectorSize();
      }

      @Override
      public int getMaxVectorSize()
      {
        return offset.getMaxVectorSize();
      }
    }

    return new QueryableMultiValueDimensionVectorSelector();
  }

  @Override
  public final VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    if (!hasMultipleValues()) {
      class DictionaryEncodedStringSingleValueVectorObjectSelector implements VectorObjectSelector
      {
        private final int[] vector = new int[offset.getMaxVectorSize()];
        private final ValType[] strings = (ValType[]) Array.newInstance(getValueClass(), offset.getMaxVectorSize());
        private int id = ReadableVectorInspector.NULL_ID;

        @Override

        public Object[] getObjectVector()
        {
          if (id == offset.getId()) {
            return strings;
          }

          if (offset.isContiguous()) {
            column.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
          } else {
            column.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
          }
          for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
            strings[i] = lookupName(vector[i]);
          }
          id = offset.getId();

          return strings;
        }

        @Override
        public int getMaxVectorSize()
        {
          return offset.getMaxVectorSize();
        }

        @Override
        public int getCurrentVectorSize()
        {
          return offset.getCurrentVectorSize();
        }
      }

      return new DictionaryEncodedStringSingleValueVectorObjectSelector();
    } else {
      throw new UnsupportedOperationException("Multivalue string object selector not implemented yet");
    }
  }

  @Override
  public final void close() throws IOException
  {
    CloseableUtils.closeAll(cachedDictionary, column, multiValueColumn);
  }

  protected abstract Class<ValType> getValueClass();

  @Nullable
  protected abstract ValType convertFromStringName(@Nullable String name);

  @Nullable
  protected abstract String convertToStringName(@Nullable ValType name);
}
