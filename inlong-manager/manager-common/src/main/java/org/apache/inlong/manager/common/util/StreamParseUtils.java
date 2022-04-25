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

package org.apache.inlong.manager.common.util;

import com.google.gson.Gson;
import org.apache.inlong.manager.common.enums.TransformType;
import org.apache.inlong.manager.common.pojo.stream.StreamPipeline;
import org.apache.inlong.manager.common.pojo.transform.TransformDefinition;
import org.apache.inlong.manager.common.pojo.transform.deduplication.DeDuplicationDefinition;
import org.apache.inlong.manager.common.pojo.transform.filter.FilterDefinition;
import org.apache.inlong.manager.common.pojo.transform.joiner.JoinerDefinition;
import org.apache.inlong.manager.common.pojo.transform.replacer.StringReplacerDefinition;
import org.apache.inlong.manager.common.pojo.transform.splitter.SplitterDefinition;

public class StreamParseUtils {

    private static Gson gson = new Gson();

    public static TransformDefinition parseTransformDefinition(String transformDefinition,
            TransformType transformType) {
        switch (transformType) {
            case FILTER:
                return gson.fromJson(transformDefinition, FilterDefinition.class);
            case JOINER:
                return gson.fromJson(transformDefinition, JoinerDefinition.class);
            case SPLITTER:
                return gson.fromJson(transformDefinition, SplitterDefinition.class);
            case DE_DUPLICATION:
                return gson.fromJson(transformDefinition, DeDuplicationDefinition.class);
            case STRING_REPLACER:
                return gson.fromJson(transformDefinition, StringReplacerDefinition.class);
            default:
                throw new IllegalArgumentException(String.format("Unsupported transformType for %s", transformType));
        }
    }

    public static StreamPipeline parseStreamPipeline(String tempView, String inlongStreamId) {
        Preconditions.checkNotEmpty(tempView,
                String.format(" should not be null for streamId=%s", inlongStreamId));
        return gson.fromJson(tempView, StreamPipeline.class);
    }

}
