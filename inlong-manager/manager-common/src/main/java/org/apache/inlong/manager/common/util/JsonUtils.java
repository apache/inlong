/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.inlong.manager.common.util;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.exceptions.JsonException;
import org.reflections.Reflections;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@UtilityClass
@Slf4j
public class JsonUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String PROJECT_PACKAGE = "org.apache.inlong.manager.common.pojo";

    static {
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        initJsonTypeDefine(OBJECT_MAPPER);
    }

    @SneakyThrows
    public static String toJsonString(Object object) {
        return OBJECT_MAPPER.writeValueAsString(object);
    }

    @SneakyThrows
    public static byte[] toJsonByte(Object object) {
        return OBJECT_MAPPER.writeValueAsBytes(object);
    }

    public static <T> T parseObject(String text, Class<T> clazz) {
        if (StringUtils.isEmpty(text)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(text, clazz);
        } catch (IOException e) {
            log.error("json parse err,json:{}", text, e);
            throw new JsonException(e);
        }
    }

    public static <T> T parseObject(byte[] bytes, Class<T> clazz) {
        if (ArrayUtils.isEmpty(bytes)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(bytes, clazz);
        } catch (IOException e) {
            log.error("json parse err,json:{}", bytes, e);
            throw new JsonException(e);
        }
    }

    public static <T> T parseObject(String text, JavaType javaType) {
        try {
            return OBJECT_MAPPER.readValue(text, javaType);
        } catch (IOException e) {
            log.error("json parse err,json:{}", text, e);
            throw new JsonException(e);
        }
    }

    public static <T> T parseObject(String text, TypeReference<T> typeReference) {
        try {
            return OBJECT_MAPPER.readValue(text, typeReference);
        } catch (IOException e) {
            log.error("json parse err,json:{}", text, e);
            throw new JsonException(e);
        }
    }

    public static <T> List<T> parseArray(String text, Class<T> clazz) {
        if (StringUtils.isEmpty(text)) {
            return new ArrayList<>();
        }
        try {
            return OBJECT_MAPPER.readValue(text,
                    OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (IOException e) {
            log.error("json parse err,json:{}", text, e);
            throw new JsonException(e);
        }
    }

    public static JsonNode parseTree(String text) {
        try {
            return OBJECT_MAPPER.readTree(text);
        } catch (IOException e) {
            log.error("json parse err,json:{}", text, e);
            throw new JsonException(e);
        }
    }

    public static JsonNode parseTree(byte[] text) {
        try {
            return OBJECT_MAPPER.readTree(text);
        } catch (IOException e) {
            log.error("json parse err,json:{}", text, e);
            throw new JsonException(e);
        }
    }

    /**
     * Init all classes that marked with JsonTypeInfo annotation
     */
    public static void initJsonTypeDefine(ObjectMapper objectMapper) {
        Reflections reflections = new Reflections(PROJECT_PACKAGE);
        Set<Class<?>> typeSet = reflections.getTypesAnnotatedWith(JsonTypeInfo.class);

        // Get all subtype of class which marked JsonTypeInfo annotation
        for (Class<?> type : typeSet) {
            Set<?> clazzSet = reflections.getSubTypesOf(type);
            if (CollectionUtils.isEmpty(clazzSet)) {
                continue;
            }
            // Register all subclasses
            // Skip the interface and abstract class
            // Get the JsonTypeDefine annotation
            // Register the subtype and use the NamedType to build the relation
            clazzSet.stream()
                    .map(obj -> (Class<?>) obj)
                    .filter(clazz -> !clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers()))
                    .forEach(clazz -> {
                        JsonTypeDefine extendClassDefine = clazz.getAnnotation(JsonTypeDefine.class);
                        if (extendClassDefine == null) {
                            return;
                        }
                        objectMapper.registerSubtypes(new NamedType(clazz, extendClassDefine.value()));
                    });
        }
    }
}
