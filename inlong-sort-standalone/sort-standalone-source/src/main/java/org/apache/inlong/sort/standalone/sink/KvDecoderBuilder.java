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

package org.apache.inlong.sort.standalone.sink;

import org.apache.inlong.common.pojo.sort.dataflow.SourceConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.DataTypeConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.KvConfig;
import org.apache.inlong.sdk.transform.decode.SourceDecoder;
import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSourceInfo;

import java.util.List;
import java.util.stream.Collectors;

public class KvDecoderBuilder extends BaseDecoderBuilder {

    @Override
    public SourceDecoder<String> createSourceDecoder(SourceConfig sourceConfig) {
        List<FieldInfo> fieldInfoList = sourceConfig.getFieldConfigs()
                .stream()
                .map(this::convertToTransformFieldInfo)
                .collect(Collectors.toList());

        DataTypeConfig dataTypeConfig = sourceConfig.getDataTypeConfig();
        if (dataTypeConfig instanceof KvConfig) {
            KvConfig kvConfig = (KvConfig) dataTypeConfig;
            KvSourceInfo kvSourceInfo = KvSourceInfo.builder()
                    .charset(sourceConfig.getEncodingType())
                    .fields(fieldInfoList)
                    .kvDelimiter(kvConfig.getKvSplitter())
                    .entryDelimiter(kvConfig.getEntrySplitter())
                    .lineDelimiter(kvConfig.getLineSeparator())
                    .escapeChar(kvConfig.getEscapeChar())
                    .build();
            return SourceDecoderFactory.createKvDecoder(kvSourceInfo);
        }
        return null;
    }
}
