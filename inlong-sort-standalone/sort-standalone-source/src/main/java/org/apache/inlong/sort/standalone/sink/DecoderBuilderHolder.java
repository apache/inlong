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

import org.apache.inlong.common.pojo.sort.dataflow.dataType.CsvConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.KvConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.PbConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DecoderBuilderHolder
 * 
 */
public class DecoderBuilderHolder {

    private static final Map<String, IDecoderBuilder> builderMap = new ConcurrentHashMap<>();
    static {
        builderMap.put(CsvConfig.class.getSimpleName(), new CsvDecoderBuilder());
        builderMap.put(KvConfig.class.getSimpleName(), new KvDecoderBuilder());
        builderMap.put(PbConfig.class.getSimpleName(), new PbDecoderBuilder());
    }

    public static IDecoderBuilder getBuilder(String dataTypeConfig) {
        IDecoderBuilder builder = builderMap.get(dataTypeConfig);
        if (builder != null) {
            return builder;
        }
        throw new IllegalArgumentException("do not support data type=" + dataTypeConfig);
    }

    public static void setBuilder(String dataTypeConfig, IDecoderBuilder builder) {
        if (builder == null) {
            return;
        }
        builderMap.put(dataTypeConfig, builder);
    }
}
