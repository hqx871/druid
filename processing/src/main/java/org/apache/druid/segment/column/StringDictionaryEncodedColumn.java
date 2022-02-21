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

import org.apache.druid.segment.data.CachingIndexed;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 *
 */
public class StringDictionaryEncodedColumn extends BaseDictionaryEncodedColumn<String>
{
  public StringDictionaryEncodedColumn(
      @Nullable ColumnarInts singleValueColumn,
      @Nullable ColumnarMultiInts multiValueColumn,
      CachingIndexed<String> dictionary,
      Indexed<ByteBuffer> dictionaryUtf8
  )
  {
    super(singleValueColumn, multiValueColumn, dictionary, dictionaryUtf8);
  }

  @Override
  protected Class<String> getValueClass()
  {
    return String.class;
  }

  @Override
  protected String convertFromStringName(@Nullable String name)
  {
    return name;
  }

  @Override
  protected String convertToStringName(@Nullable String name)
  {
    return name;
  }
}
