/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.formats.inlongmsg;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * AbstractInLongMsgDecodingFormat.
 */
public abstract class AbstractInLongMsgDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    protected List<String> metadataKeys;

    public AbstractInLongMsgDecodingFormat() {
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Arrays.stream(InLongMsgMetadata.ReadableMetadata.values())
                .collect(Collectors.toMap(
                        InLongMsgMetadata.ReadableMetadata::getKey,
                        InLongMsgMetadata.ReadableMetadata::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }
}
