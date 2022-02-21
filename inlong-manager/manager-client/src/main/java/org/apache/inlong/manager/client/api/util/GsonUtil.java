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

package org.apache.inlong.manager.client.api.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GsonUtil {

    private static Gson gson;
    private static Gson gsonWithNull;

    private static JsonDeserializer<Date> dataJsonDeserializer = new JsonDeserializer<Date>() {

        private Pattern pattern = Pattern.compile("[0-9]+.?[0-9E]+");

        @SneakyThrows
        @Override
        public Date deserialize(JsonElement json, Type typeOfT,
                JsonDeserializationContext context) throws JsonParseException {
            String dateStr = json.getAsString();
            Matcher isNum = pattern.matcher(dateStr);
            if (isNum.matches()) {
                long timestamp = Double.valueOf(dateStr).longValue();
                return new Date(timestamp);
            } else {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return formatter.parse(dateStr);
            }
        }
    };

    static {
        final GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(Date.class, dataJsonDeserializer);
        gson = builder.create();
        final GsonBuilder builderWithNull = new GsonBuilder().serializeNulls();
        builder.registerTypeAdapter(Date.class, dataJsonDeserializer);
        gsonWithNull = builderWithNull.create();
    }

    private GsonUtil() {
    }

    public static Gson getGson() {
        return gson;
    }

    public static Gson getGsonWithNull() {
        return gsonWithNull;
    }

    public static void jsonObjectToMap(Map<String, String> parameterMap, JsonObject jsonObject) {
        String key = null;
        String value = null;
        Iterator<String> iterator = jsonObject.keySet().iterator();
        while (iterator.hasNext()) {
            key = iterator.next();
            JsonElement jsonElement = jsonObject.get(key);

            if (jsonElement instanceof JsonObject || jsonElement instanceof JsonArray) {
                value = getGson().toJson(jsonElement);
            } else {
                value = jsonElement.getAsString();
            }
            parameterMap.put(key, (null == value) ? "" : value);
        }
    }

    public static JsonObject getJsonObjectFromObject(Object object) {
        JsonElement element = getGson().fromJson(getGson().toJson(object), JsonElement.class);
        return element.getAsJsonObject();
    }

    public static String toJson(Object src) {
        return gson.toJson(src);
    }

    public static <T> T fromJson(String json, Type typeOfT) throws JsonSyntaxException {
        return gson.fromJson(json, typeOfT);
    }

    public static String toJsonHasNull(Object src) {
        return gsonWithNull.toJson(src);
    }

}
