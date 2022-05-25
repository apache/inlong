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
import org.apache.inlong.manager.client.api.source.AgentFileSource;
import org.apache.inlong.manager.client.api.source.KafkaSource;
import org.apache.inlong.manager.client.api.source.MySQLBinlogSource;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.stream.StreamSource;

import java.lang.reflect.Type;

/**
 * Stream source adapter.
 */
public class StreamSourceAdapter implements JsonDeserializer<StreamSource> {

    @Override
    public StreamSource deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context)
            throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        String sourceType = jsonObject.get("sourceType").getAsString();
        Gson gson = GsonUtils.GSON;
        try {
            switch (sourceType) {
                case SourceType.SOURCE_KAFKA:
                    return gson.fromJson(jsonElement, (Type) Class.forName((KafkaSource.class).getName()));
                case SourceType.SOURCE_BINLOG:
                    return gson.fromJson(jsonElement, (Type) Class.forName((MySQLBinlogSource.class).getName()));
                case SourceType.SOURCE_FILE:
                    return gson.fromJson(jsonElement, (Type) Class.forName((AgentFileSource.class).getName()));
                default:
                    throw new IllegalArgumentException(
                            String.format("Unsupported source type=%s for Inlong", sourceType));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}