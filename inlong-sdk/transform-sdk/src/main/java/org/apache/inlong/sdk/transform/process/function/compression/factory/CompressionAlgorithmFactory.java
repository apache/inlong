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

package org.apache.inlong.sdk.transform.process.function.compression.factory;

import org.apache.inlong.sdk.transform.process.function.compression.handler.CompressHandler;
import org.apache.inlong.sdk.transform.process.function.compression.handler.DeflaterCompress;
import org.apache.inlong.sdk.transform.process.function.compression.handler.GzipCompress;
import org.apache.inlong.sdk.transform.process.function.compression.handler.ZipCompress;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CompressionAlgorithmFactory {

    private static Map<String, CompressHandler> compressMap = new ConcurrentHashMap<>();

    static {
        compressMap.put("deflater", new DeflaterCompress());
        compressMap.put("gzip", new GzipCompress());
        compressMap.put("zip", new ZipCompress());
    }

    public static CompressHandler getCompressHandlerByName(String name) {
        return compressMap.get(name);
    }
}
