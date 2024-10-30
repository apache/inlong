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

package org.apache.inlong.sdk.transform.encode;

import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.MapSinkInfo;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.converter.TypeConverter;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class MapSinkEncoder extends SinkEncoder<Map<String, Object>> {

    private final MapSinkInfo sinkInfo;
    private final Map<String, TypeConverter> converters;

    public MapSinkEncoder(MapSinkInfo sinkInfo) {
        super(sinkInfo.getFields());
        this.sinkInfo = sinkInfo;
        this.converters = sinkInfo.getFields()
                .stream()
                .collect(Collectors.toMap(FieldInfo::getName,
                        info -> info.getConverter() == null ? TypeConverter.DefaultTypeConverter()
                                : info.getConverter()));
    }

    @Override
    public Map<String, Object> encode(SinkData sinkData, Context context) {
        Map<String, Object> esMap = new HashMap<>();
        for (FieldInfo fieldInfo : fields) {
            String fieldName = fieldInfo.getName();
            String strValue = sinkData.getField(fieldName);
            TypeConverter converter = converters.get(fieldName);
            if (converter == null) {
                esMap.put(fieldName, strValue);
                continue;
            }

            try {
                esMap.put(fieldName, converter.convert(strValue));
            } catch (Throwable t) {
                esMap.put(fieldName, null);
            }
        }

        return esMap;
    }
}
