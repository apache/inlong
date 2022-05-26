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
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.apache.inlong.manager.client.api.sink.ClickHouseSink;
import org.apache.inlong.manager.client.api.sink.HiveSink;
import org.apache.inlong.manager.client.api.sink.KafkaSink;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.stream.StreamSink;

import java.lang.reflect.Type;

/**
 * Stream sink adapter.
 */
public class StreamSinkAdapter implements JsonDeserializer<StreamSink> {

    @Override
    public StreamSink deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context)
            throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        String sinkType = jsonObject.get("sinkType").getAsString();
        Gson gson = GsonUtils.GSON;
        try {
            switch (sinkType) {
                case SinkType.SINK_HIVE:
                    return gson.fromJson(jsonElement, (Type) Class.forName((HiveSink.class).getName()));
                case SinkType.SINK_KAFKA:
                    return gson.fromJson(jsonElement, (Type) Class.forName((KafkaSink.class).getName()));
                case SinkType.SINK_CLICKHOUSE:
                    return gson.fromJson(jsonElement, (Type) Class.forName((ClickHouseSink.class).getName()));
                default:
                    throw new IllegalArgumentException(String.format("Unsupported sink type=%s for Inlong", sinkType));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
