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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRow;
import org.apache.druid.segment.incremental.IncrementalIndexRowHolder;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public abstract class MultiValueDictionaryDimensionIndexer<ValType extends Comparable<ValType>>
    extends DictionaryEncodedColumnIndexer<int[], ValType>
{
  protected final MultiValueHandling multiValueHandling;
  protected final boolean hasBitmapIndexes;
  protected final boolean hasSpatialIndexes;
  protected final boolean useMaxMemoryEstimates;
  protected final ColumnType columnType;
  protected volatile boolean hasMultipleValues = false;

  public MultiValueDictionaryDimensionIndexer(
      DimensionDictionary<ValType> dictionary,
      MultiValueHandling multiValueHandling,
      boolean hasBitmapIndexes,
      boolean hasSpatialIndexes,
      boolean useMaxMemoryEstimates,
      ColumnType columnType
  )
  {
    super(dictionary);
    this.multiValueHandling = multiValueHandling == null ? MultiValueHandling.ofDefault() : multiValueHandling;
    this.hasBitmapIndexes = hasBitmapIndexes;
    this.hasSpatialIndexes = hasSpatialIndexes;
    this.useMaxMemoryEstimates = useMaxMemoryEstimates;
    this.columnType = columnType;
  }

  @Override
  public EncodedKeyComponent<int[]> processRowValsToUnsortedEncodedKeyComponent(
      @Nullable Object dimValues,
      boolean reportParseExceptions
  )
  {
    final int[] encodedDimensionValues;
    final int oldDictSize = dimLookup.size();
    final long oldDictSizeInBytes = useMaxMemoryEstimates ? 0 : dimLookup.sizeInBytes();

    if (dimValues == null) {
      final int nullId = dimLookup.getId(null);
      encodedDimensionValues = nullId == DimensionDictionary.ABSENT_VALUE_ID
                               ? new int[]{dimLookup.add(null)}
                               : new int[]{nullId};
    } else if (dimValues instanceof List) {
      List<Object> dimValuesList = (List<Object>) dimValues;
      if (dimValuesList.isEmpty()) {
        dimLookup.add(null);
        encodedDimensionValues = IntArrays.EMPTY_ARRAY;
      } else if (dimValuesList.size() == 1) {
        //encodedDimensionValues = new int[]{dimLookup.add(emptyToNullIfNeeded(dimValuesList.get(0)))};
        encodedDimensionValues = new int[]{dimLookup.add(parseValueAndReplaceWithDefault(dimValuesList.get(0)))};
      } else {
        hasMultipleValues = true;
        final List<ValType> dimensionValues = new ArrayList<>(dimValuesList.size());
        for (int i = 0; i < dimValuesList.size(); i++) {
          //dimensionValues[i] = emptyToNullIfNeeded(dimValuesList.get(i));
          dimensionValues.add(parseValueAndReplaceWithDefault(dimValuesList.get(i)));
        }
        if (multiValueHandling.needSorting()) {
          // Sort multival row by their unencoded values first.
          //Arrays.sort(dimensionValues, Comparators.naturalNullsFirst());
          dimensionValues.sort(Comparators.naturalNullsFirst());
        }

        final int[] retVal = new int[dimensionValues.size()];

        int prevId = -1;
        int pos = 0;
        for (ValType dimensionValue : dimensionValues) {
          if (multiValueHandling != MultiValueHandling.SORTED_SET) {
            retVal[pos++] = dimLookup.add(dimensionValue);
            continue;
          }
          int index = dimLookup.add(dimensionValue);
          if (index != prevId) {
            prevId = retVal[pos++] = index;
          }
        }

        encodedDimensionValues = pos == retVal.length ? retVal : Arrays.copyOf(retVal, pos);
      }
    } else {
      encodedDimensionValues = new int[]{dimLookup.add(parseValueAndReplaceWithDefault(dimValues))};
    }

    // If dictionary size has changed, the sorted lookup is no longer valid.
    if (oldDictSize != dimLookup.size()) {
      sortedLookup = null;
    }

    long effectiveSizeBytes;
    if (useMaxMemoryEstimates) {
      effectiveSizeBytes = estimateEncodedKeyComponentSize(encodedDimensionValues);
    } else {
      // size of encoded array + dictionary size change
      effectiveSizeBytes = 16L + (long) encodedDimensionValues.length * Integer.BYTES
                           + (dimLookup.sizeInBytes() - oldDictSizeInBytes);
    }
    return new EncodedKeyComponent<>(encodedDimensionValues, effectiveSizeBytes);
  }

  /**
   * Estimates size of the given key component.
   * <p>
   * Deprecated method. Use {@link #processRowValsToUnsortedEncodedKeyComponent(Object, boolean)}
   * and {@link EncodedKeyComponent#getEffectiveSizeBytes()}.
   */
  public abstract long estimateEncodedKeyComponentSize(int[] key);

  @Override
  public int compareUnsortedEncodedKeyComponents(int[] lhs, int[] rhs)
  {
    int lhsLen = lhs.length;
    int rhsLen = rhs.length;

    int lenCompareResult = Ints.compare(lhsLen, rhsLen);
    if (lenCompareResult != 0) {
      // if the values don't have the same length, check if we're comparing [] and [null], which are equivalent
      if (lhsLen + rhsLen == 1) {
        int[] longerVal = rhsLen > lhsLen ? rhs : lhs;
        if (longerVal[0] == dimLookup.getIdForNull()) {
          return 0;
        } else {
          //noinspection ArrayEquality -- longerVal is explicitly set to only lhs or rhs
          return longerVal == lhs ? 1 : -1;
        }
      }
    }

    int valsIndex = 0;
    int lenToCompare = Math.min(lhsLen, rhsLen);
    while (valsIndex < lenToCompare) {
      int lhsVal = lhs[valsIndex];
      int rhsVal = rhs[valsIndex];
      if (lhsVal != rhsVal) {
        final ValType lhsValActual = getActualValue(lhsVal, false);
        final ValType rhsValActual = getActualValue(rhsVal, false);
        int valueCompareResult = 0;
        if (lhsValActual != null && rhsValActual != null) {
          valueCompareResult = lhsValActual.compareTo(rhsValActual);
        } else if (lhsValActual == null ^ rhsValActual == null) {
          valueCompareResult = lhsValActual == null ? -1 : 1;
        }
        if (valueCompareResult != 0) {
          return valueCompareResult;
        }
      }
      ++valsIndex;
    }

    return lenCompareResult;
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(int[] lhs, int[] rhs)
  {
    return Arrays.equals(lhs, rhs);
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(int[] key)
  {
    return Arrays.hashCode(key);
  }

  @Override
  public ColumnCapabilities getColumnCapabilities()
  {
    ColumnCapabilitiesImpl capabilites = new ColumnCapabilitiesImpl().setType(columnType)
                                                                     .setHasBitmapIndexes(hasBitmapIndexes)
                                                                     .setHasSpatialIndexes(hasSpatialIndexes)
                                                                     .setDictionaryValuesUnique(true)
                                                                     .setDictionaryValuesSorted(false);

    // Strings are opportunistically multi-valued, but the capabilities are initialized as 'unknown', since a
    // multi-valued row might be processed at any point during ingestion.
    // We only explicitly set multiple values if we are certain that there are multiple values, otherwise, a race
    // condition might occur where this indexer might process a multi-valued row in the period between obtaining the
    // capabilities, and actually processing the rows with a selector. Leaving as unknown allows the caller to decide
    // how to handle this.
    if (hasMultipleValues) {
      capabilites.setHasMultipleValues(true);
    }
    // Likewise, only set dictionaryEncoded if explicitly if true for a similar reason as multi-valued handling. The
    // dictionary is populated as rows are processed, but there might be implicit default values not accounted for in
    // the dictionary yet. We can be certain that the dictionary has an entry for every value if either of
    //    a) we have already processed an explitic default (null) valued row for this column
    //    b) the processing was not 'sparse', meaning that this indexer has processed an explict value for every row
    // is true.
    final boolean allValuesEncoded = dictionaryEncodesAllValues();
    if (allValuesEncoded) {
      capabilites.setDictionaryEncoded(true);
    }

    if (isSparse || dimLookup.getIdForNull() != DimensionDictionary.ABSENT_VALUE_ID) {
      capabilites.setHasNulls(true);
    }
    return capabilites;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      final DimensionSpec spec,
      final IncrementalIndexRowHolder currEntry,
      final IncrementalIndex.DimensionDesc desc
  )
  {
    final ExtractionFn extractionFn = spec.getExtractionFn();

    final int dimIndex = desc.getIndex();

    // maxId is used in concert with getLastRowIndex() in IncrementalIndex to ensure that callers do not encounter
    // rows that contain IDs over the initially-reported cardinality. The main idea is that IncrementalIndex establishes
    // a watermark at the time a cursor is created, and doesn't allow the cursor to walk past that watermark.
    //
    // Additionally, this selector explicitly blocks knowledge of IDs past maxId that may occur from other causes
    // (for example: nulls getting generated for empty arrays, or calls to lookupId).
    final int maxId = getCardinality();

    class IndexerDimensionSelector implements DimensionSelector, IdLookup<ValType>
    {
      private final ArrayBasedIndexedInts indexedInts = new ArrayBasedIndexedInts();

      @Nullable
      @MonotonicNonNull
      private int[] nullIdIntArray;

      @Override
      public IndexedInts getRow()
      {
        final Object[] dims = currEntry.get().getDims();

        int[] indices;
        if (dimIndex < dims.length) {
          indices = (int[]) dims[dimIndex];
        } else {
          indices = null;
        }

        int[] row = null;
        int rowSize = 0;

        // usually due to currEntry's rowIndex is smaller than the row's rowIndex in which this dim first appears
        if (indices == null || indices.length == 0) {
          if (hasMultipleValues) {
            row = IntArrays.EMPTY_ARRAY;
            rowSize = 0;
          } else {
            final int nullId = getEncodedValue(null, false);
            if (nullId >= 0 && nullId < maxId) {
              // null was added to the dictionary before this selector was created; return its ID.
              if (nullIdIntArray == null) {
                nullIdIntArray = new int[]{nullId};
              }
              row = nullIdIntArray;
              rowSize = 1;
            } else {
              // null doesn't exist in the dictionary; return an empty array.
              // Choose to use ArrayBasedIndexedInts later, instead of special "empty" IndexedInts, for monomorphism
              row = IntArrays.EMPTY_ARRAY;
              rowSize = 0;
            }
          }
        }

        if (row == null && indices != null && indices.length > 0) {
          row = indices;
          rowSize = indices.length;
        }

        indexedInts.setValues(row, rowSize);
        return indexedInts;
      }

      @Override
      public ValueMatcher makeValueMatcher(final String value)
      {
        if (extractionFn == null) {
          //final int valueId = lookupId(value);
          final int valueId = lookupId(value == null ? null : parseStringValue(value));
          if (valueId >= 0 || value == null) {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                Object[] dims = currEntry.get().getDims();
                if (dimIndex >= dims.length) {
                  return value == null;
                }

                int[] dimsInt = (int[]) dims[dimIndex];
                if (dimsInt == null || dimsInt.length == 0) {
                  return value == null;
                }

                for (int id : dimsInt) {
                  if (id == valueId) {
                    return true;
                  }
                }
                return false;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                // nothing to inspect
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
        final BitSet checkedIds = new BitSet(maxId);
        final BitSet matchingIds = new BitSet(maxId);
        final boolean matchNull = predicate.apply(null);

        // Lazy matcher; only check an id if matches() is called.
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            Object[] dims = currEntry.get().getDims();
            if (dimIndex >= dims.length) {
              return matchNull;
            }

            int[] dimsInt = (int[]) dims[dimIndex];
            if (dimsInt == null || dimsInt.length == 0) {
              return matchNull;
            }

            for (int id : dimsInt) {
              if (checkedIds.get(id)) {
                if (matchingIds.get(id)) {
                  return true;
                }
              } else {
                final boolean matches = predicate.apply(lookupName(id));
                checkedIds.set(id);
                if (matches) {
                  matchingIds.set(id);
                  return true;
                }
              }
            }
            return false;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            // nothing to inspect
          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        return maxId;
      }

      @Override
      public String lookupName(int id)
      {
        if (id >= maxId) {
          // Sanity check; IDs beyond maxId should not be known to callers. (See comment above.)
          throw new ISE("id[%d] >= maxId[%d]", id, maxId);
        }
        final String strValue = convertToStringName(getActualValue(id, false));
        return extractionFn == null ? strValue : extractionFn.apply(strValue);
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return dictionaryEncodesAllValues();
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
          throw new UnsupportedOperationException(
              "cannot perform lookup when applying an extraction function"
          );
        }

        final int id = getEncodedValue(name, false);

        if (id < maxId) {
          return id;
        } else {
          // Can happen if a value was added to our dimLookup after this selector was created. Act like it
          // doesn't exist.
          return DimensionDictionary.ABSENT_VALUE_ID;
        }
      }

      @SuppressWarnings("deprecation")
      @Nullable
      @Override
      public Object getObject()
      {
        IncrementalIndexRow key = currEntry.get();
        if (key == null) {
          return null;
        }

        Object[] dims = key.getDims();
        if (dimIndex >= dims.length) {
          return null;
        }

        return convertUnsortedEncodedKeyComponentToActualList((int[]) dims[dimIndex]);
      }

      @SuppressWarnings("deprecation")
      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    }
    return new IndexerDimensionSelector();
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
      List<ValType> rowArray = new ArrayList<>(key.length);
      for (int id : key) {
        ValType val = getActualValue(id, false);
        rowArray.add(val);
      }
      return rowArray;
    }
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      int[] key,
      int rowNum,
      MutableBitmap[] bitmapIndexes,
      BitmapFactory factory
  )
  {
    if (!hasBitmapIndexes) {
      throw new UnsupportedOperationException("This column does not include bitmap indexes");
    }

    for (int dimValIdx : key) {
      if (bitmapIndexes[dimValIdx] == null) {
        bitmapIndexes[dimValIdx] = factory.makeEmptyMutableBitmap();
      }
      bitmapIndexes[dimValIdx].add(rowNum);
    }
  }

  protected abstract Class<ValType> getValueClass();

  @Nullable
  protected abstract String convertToStringName(@Nullable ValType name);

  @Nullable
  protected abstract ValType parseValueAndReplaceWithDefault(@Nullable Object value);

  @Nullable
  protected abstract ValType parseStringValue(@Nullable String value);
}
