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
import org.apache.inlong.manager.common.enums.DataFormat;
import org.apache.inlong.manager.common.enums.DataSeparator;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.sort.BaseSortConf;
import org.apache.inlong.manager.common.pojo.stream.StreamSink;
import org.apache.inlong.manager.common.pojo.stream.StreamSource;

import java.nio.charset.Charset;

/**
 * Util of gson for register each type of adapter, such as charsetAdapter, streamSourceAdapter, etc.
 */
public class GsonUtils {

    /**
     * Init gson instance with register type adapter.
     */
    public static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(InlongGroupInfo.class, new InlongGroupInfoAdapter())
            .registerTypeAdapter(BaseSortConf.class, new SortBaseConfAdapter())
            .registerTypeAdapter(Charset.class, new CharsetAdapter())
            .registerTypeAdapter(DataSeparator.class, new SeparatorAdapter())
            .registerTypeAdapter(DataFormat.class, new DataFormatAdapter())
            .registerTypeAdapter(StreamSource.class, new StreamSourceAdapter())
            .registerTypeAdapter(StreamSink.class, new StreamSinkAdapter())
            .create();

}
