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

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.exceptions.JsonException;

/**
 * JSON utils
 */
@Slf4j
public class JsonUtils {

    public static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * The instance need to transform to string
     *
     * @param obj instance need to transform
     * @return JSON string after transform
     */
    public static String toJson(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj.getClass() == String.class) {
            return (String) obj;
        }
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("JSON transform error: " + obj, e);
            throw new JsonException("JSON transform error");
        }
    }

    /**
     * Transform the string to specify type
     *
     * @param json JSON string
     * @param type specify type
     * @param <T> type
     * @return instance of type T
     */
    public static <T> T parse(String json, TypeReference<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            log.error("JSON transform error: " + json, e);
            throw new JsonException("JSON transform error");
        }
    }

    /**
     * Transform the string to JsonNode
     *
     * @param json JSON string
     * @return JsonNode instance after transform
     */
    public static JsonNode parse(String json) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return MAPPER.readTree(json);
        } catch (IOException e) {
            log.error("JSON transform error: " + json, e);
            throw new JsonException("JSON transform error");
        }
    }

    /**
     * Transform JSON string to Java instance
     *
     * @param json JSON string
     * @param tClass Java instance type
     * @param <T> type
     * @return java instance after transform
     */
    public static <T> T parse(String json, Class<T> tClass) {
        try {
            return MAPPER.readValue(json, tClass);
        } catch (IOException e) {
            log.error("JSON transform error: " + json, e);
            throw new JsonException("JSON transform error");
        }
    }

    public static <T> T parse(String json, JavaType javaType) {
        try {
            return MAPPER.readValue(json, javaType);
        } catch (IOException e) {
            log.error("JSON transform error: " + json, e);
            throw new JsonException("JSON transform error");
        }
    }

    /**
     * Transform JSON string to List
     *
     * @param json JSON string
     * @param eClass element class
     * @param <E> element type
     * @return list after transform
     */
    public static <E> List<E> parseList(String json, Class<E> eClass) {
        try {
            return MAPPER.readValue(json, MAPPER.getTypeFactory().constructCollectionType(List.class, eClass));
        } catch (IOException e) {
            log.error("JSON transform error: " + json, e);
            throw new JsonException("JSON transform error");
        }
    }

    /**
     * Transform JSON string to Map
     *
     * @param json JSON string
     * @param kClass key type in Map
     * @param vClass value type in Map
     * @param <K> key type
     * @param <V> value type
     * @return map after transform
     */
    public static <K, V> Map<K, V> parseMap(String json, Class<K> kClass, Class<V> vClass) {
        try {
            return MAPPER.readValue(json, MAPPER.getTypeFactory().constructMapType(Map.class, kClass, vClass));
        } catch (IOException e) {
            log.error("JSON transform error: " + json, e);
            throw new JsonException("JSON transform error");
        }
    }

}
