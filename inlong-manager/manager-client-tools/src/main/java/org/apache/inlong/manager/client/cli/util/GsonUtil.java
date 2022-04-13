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

package org.apache.inlong.manager.client.cli.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.DataSeparator;
import org.apache.inlong.manager.client.api.MQBaseConf;
import org.apache.inlong.manager.client.api.SortBaseConf;
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.client.api.StreamSource;

import java.nio.charset.Charset;

public class GsonUtil {

    public static Gson gsonBuilder() {
        return new GsonBuilder()
                .registerTypeAdapter(MQBaseConf.class, new MQBaseConfAdapter())
                .registerTypeAdapter(SortBaseConf.class, new SortBaseConfAdapter())
                .registerTypeAdapter(Charset.class, new CharsetAdapter())
                .registerTypeAdapter(DataSeparator.class, new SeparatorAdapter())
                .registerTypeAdapter(DataFormat.class, new DataFormatAdapter())
                .registerTypeAdapter(StreamSource.class, new StreamSourceAdapter())
                .registerTypeAdapter(StreamSink.class, new StreamSinkAdapter())
                .create();
    }
}
