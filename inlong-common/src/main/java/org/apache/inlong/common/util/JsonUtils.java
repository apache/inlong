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

package org.apache.inlong.common.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class JsonUtils {
    private static final JsonParser jsonParser = new JsonParser();

    public static JsonObject parseObject(String str) {
        return jsonParser.parse(str).getAsJsonObject();
    }

    public static JsonArray parseArray(String str) {
        return jsonParser.parse(str).getAsJsonArray();
    }

    public static class JSONObject {
        private JsonObject object;
        public JSONObject() {
            this.object = new JsonObject();
        }

        public JSONObject(JSONObject j) {
            this.object = j.object;
        }

        private JSONObject(JsonObject object) {
            this.object = object;
        }

        public void put(String key, String value) {
            object.addProperty(key, value);
        }

        public void put(String key, Number n) {
            object.addProperty(key, n);
        }

        public void put(String key, Boolean b) {
            object.addProperty(key, b);
        }

        public void put(String key, Character c) {
            object.addProperty(key, c);
        }

        public void put(String key, JSONObject j) {
            object.add(key, j.object);
        }

        public void put(String key, JSONArray array) {
            object.add(key, array.array);
        }

        public boolean containsKey(String key) {
            return object.has(key);
        }

        public String getString(String key) {
            if (!object.has(key) || object.get(key).isJsonNull()) {
                return null;
            } else {
                return object.get(key).getAsString();
            }
        }

        public Integer getInteger(String key) {
            JsonElement element = object.get(key);
            if (element == null || element.isJsonNull()) {
                return null;
            } else {
                return element.getAsInt();
            }
        }

        public int getIntValue(String key) {
            JsonElement element = object.get(key);
            if (element == null || element.isJsonNull()) {
                return 0;
            } else {
                return element.getAsInt();
            }
        }

        public long getLongValue(String key) {
            JsonElement element = object.get(key);
            if (element == null || element.isJsonNull()) {
                return 0;
            } else {
                return element.getAsLong();
            }
        }

        public Long getLong(String key) {
            JsonElement element = object.get(key);
            if (element == null || element.isJsonNull()) {
                return null;
            } else {
                return element.getAsLong();
            }
        }

        public Boolean getBoolean(String key) {
            JsonElement element = object.get(key);
            if (element == null || element.isJsonNull()) {
                return null;
            } else {
                return element.getAsBoolean();
            }
        }

        public boolean getBooleanValue(String key) {
            JsonElement element = object.get(key);
            if (element == null || element.isJsonNull()) {
                return false;
            } else {
                return element.getAsBoolean();
            }
        }

        public byte getByteValue(String key) {
            JsonElement element = object.get(key);
            if (element == null || element.isJsonNull()) {
                return 0;
            } else {
                return element.getAsByte();
            }
        }

        public JSONObject getJSONObject(String key) {
            if (object == null) {
                throw new NullPointerException("object is null");
            }
            if (object.isJsonNull()) {
                throw new NullPointerException("object is JsonNull");
            }
            if (!object.has(key) || object.get(key).isJsonNull()) {
                return null;
            } else {
                return new JSONObject(object.getAsJsonObject(key));
            }
        }

        public JSONArray getJSONArray(String key) {
            if (object == null) {
                throw new NullPointerException("object is null");
            }
            if (object.isJsonNull()) {
                throw new NullPointerException("object is JsonNull");
            }
            if (!object.has(key) || object.get(key).isJsonNull()) {
                return null;
            } else {
                return new JSONArray(object.getAsJsonArray(key));
            }
        }

        public int size() {
            return object.size();
        }

        public Set<String> keySet() {
            return object.keySet();
        }

        public String toJSONString() {
            return object.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (!(obj instanceof JSONObject)) {
                return false;
            }

            try {
                JSONObject json = ((JSONObject) obj);
                return Objects.equals(this.object, json.object);
            } catch (ClassCastException|NullPointerException e) {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return this.object.hashCode();
        }

        @Override
        public String toString() {
            return toJSONString();
        }

        public boolean isEmpty() {
            return object.size() == 0;
        }

        public static JSONObject parseObject(String jsonStr) {
            return new JSONObject(JsonUtils.parseObject(jsonStr));
        }

    }

    public static class JSONArray implements Iterable<Object> {
        private JsonArray array;
        public JSONArray() {
            this.array = new JsonArray();
        }

        private JSONArray(JsonArray array) {
            this.array = array;
        }

        public void add(Boolean b) {
            this.array.add(b);
        }

        public void add(Number n) {
            this.array.add(n);
        }

        public void add(String s) {
            this.array.add(s);
        }

        public void add(JSONObject object) {
            array.add(object.object);
        }

        public String toJSONString() {
            return array.toString();
        }

        @Override
        public String toString() {
            return toJSONString();
        }

        public int size() {
            return array.size();
        }

        @Override
        public Iterator<Object> iterator() {
            ArrayList<Object> list = new ArrayList<>();
            Iterator<JsonElement> itr = array.iterator();
            itr.forEachRemaining( jsonElement -> {
                JsonObject object = jsonElement.getAsJsonObject();
                JSONObject wrapperObj = new JSONObject(object);
                list.add(wrapperObj);
            });
            return list.iterator();
        }
    }
}

