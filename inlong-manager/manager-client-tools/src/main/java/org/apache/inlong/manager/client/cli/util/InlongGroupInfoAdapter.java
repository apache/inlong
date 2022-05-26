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
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.none.InlongNoneMqInfo;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongTdmqPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.tube.InlongTubeInfo;

import java.lang.reflect.Type;

/**
 * Inlong group info adapter for JSON deserialize.
 */
public class InlongGroupInfoAdapter implements JsonDeserializer<InlongGroupInfo> {

    @Override
    public InlongGroupInfo deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context)
            throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        String mqType = jsonObject.get("mqType").getAsString();
        try {
            switch (mqType) {
                case "PULSAR":
                    return new Gson().fromJson(jsonElement, (Type) Class.forName((InlongPulsarInfo.class).getName()));
                case "TUBE":
                    return new Gson().fromJson(jsonElement, (Type) Class.forName((InlongTubeInfo.class).getName()));
                case "TDMQ_PULSAR":
                    return new Gson().fromJson(jsonElement,
                            (Type) Class.forName((InlongTdmqPulsarInfo.class).getName()));
                case "NONE":
                    return new Gson().fromJson(jsonElement, (Type) Class.forName((InlongNoneMqInfo.class).getName()));
                default:
                    throw new IllegalArgumentException(String.format("Unsupported mq type=%s for Inlong", mqType));
            }
        } catch (Exception e) {
            throw new JsonParseException(e);
        }
    }
}
