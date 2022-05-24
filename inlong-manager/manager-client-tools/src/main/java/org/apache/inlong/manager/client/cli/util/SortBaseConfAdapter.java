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
import org.apache.inlong.manager.common.pojo.sort.BaseSortConf;
import org.apache.inlong.manager.common.pojo.sort.FlinkSortConf;
import org.apache.inlong.manager.common.pojo.sort.UserDefinedSortConf;

import java.lang.reflect.Type;

/**
 * Sort base config adapter for JSON deserialize.
 */
public class SortBaseConfAdapter implements JsonDeserializer<BaseSortConf> {

    @Override
    public BaseSortConf deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context)
            throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        String sortType = jsonObject.get("type").getAsString();
        Gson gson = GsonUtils.GSON;
        try {
            switch (sortType) {
                case "FLINK":
                    return gson.fromJson(jsonElement, (Type) Class.forName((FlinkSortConf.class).getName()));
                case "USER_DEFINED":
                    return gson.fromJson(jsonElement, (Type) Class.forName((UserDefinedSortConf.class).getName()));
                default:
                    throw new IllegalArgumentException(String.format("Unsupported sort type=%s for Inlong", sortType));
            }
        } catch (Exception e) {
            throw new JsonParseException(e);
        }
    }
}
