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

package org.apache.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.nio.ByteOrder;

public class StringDictionaryEncodedColumnPartSerde extends DictionaryEncodedColumnPartSerde<String>
{
  @JsonCreator
  public static StringDictionaryEncodedColumnPartSerde createDeserializer(
      @JsonProperty("bitmapSerdeFactory") @Nullable BitmapSerdeFactory bitmapSerdeFactory,
      @NotNull @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new StringDictionaryEncodedColumnPartSerde(
        byteOrder,
        bitmapSerdeFactory != null ? bitmapSerdeFactory : new BitmapSerde.LegacyBitmapSerdeFactory(),
        null
    );
  }

  private StringDictionaryEncodedColumnPartSerde(
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      @Nullable Serializer serializer
  )
  {
    super(byteOrder, bitmapSerdeFactory, serializer);
  }

  @Override
  protected ObjectStrategy<String> getObjectStrategy()
  {
    return GenericIndexed.STRING_STRATEGY;
  }

  @Override
  protected ValueType getValueType()
  {
    return ValueType.STRING;
  }

  @Override
  protected Supplier<BitmapIndex> makeBitmapIndex(
      BitmapFactory bitmapFactory,
      GenericIndexed<ImmutableBitmap> rBitmaps,
      GenericIndexed<String> rDictionary
  )
  {
    return new StringBitmapIndexColumnPartSupplier(
        bitmapFactory,
        rBitmaps,
        rDictionary
    );
  }

  public static SerializerBuilder<String> serializerBuilder()
  {
    return new SerializerBuilder<String>()
    {
      @Override
      protected DictionaryEncodedColumnPartSerde<String> makeSerde(Serializer serializer)
      {
        return new StringDictionaryEncodedColumnPartSerde(byteOrder, bitmapSerdeFactory, serializer);
      }
    };
  }
}
