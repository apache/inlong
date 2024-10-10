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

import org.apache.inlong.sdk.transform.process.function.compression.handler.DeflaterUncompress;
import org.apache.inlong.sdk.transform.process.function.compression.handler.GzipUncompress;
import org.apache.inlong.sdk.transform.process.function.compression.handler.UncompressHandler;
import org.apache.inlong.sdk.transform.process.function.compression.handler.ZipUncompress;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UnCompressionAlgorithmFactory {

    private static Map<String, UncompressHandler> unCompressMap = new ConcurrentHashMap<>();

    static {
        unCompressMap.put("deflater", new DeflaterUncompress());
        unCompressMap.put("gzip", new GzipUncompress());
        unCompressMap.put("zip", new ZipUncompress());
    }

    public static UncompressHandler getCompressHandlerByName(String name) {
        return unCompressMap.get(name);
    }
}
