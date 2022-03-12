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

package org.apache.inlong.manager.service;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.reflections.Reflections;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.lang.reflect.Modifier;
import java.util.Set;

/**
 * Custom Command Line Runner
 */
@Component
public class CommandLineRunnerImpl implements CommandLineRunner {

    private static final String PROJECT_PACKAGE = "org.apache.inlong.manager.common.pojo";

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void run(String[] args) {
        this.initJsonTypeDefine();
    }

    /**
     * Init all classes that marked with JsonTypeInfo annotation
     */
    private void initJsonTypeDefine() {
        Reflections reflections = new Reflections(PROJECT_PACKAGE);
        Set<Class<?>> typeSet = reflections.getTypesAnnotatedWith(JsonTypeInfo.class);

        // Get all subtype of class which marked JsonTypeInfo annotation
        for (Class<?> type : typeSet) {
            Set<?> clazzSet = reflections.getSubTypesOf(type);
            if (CollectionUtils.isEmpty(clazzSet)) {
                continue;
            }
            // Register all subclasses
            for (Object obj : clazzSet) {
                Class<?> clazz = (Class<?>) obj;
                // Skip the interface and abstract class
                if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
                    continue;
                }
                // Get the JsonTypeDefine annotation
                JsonTypeDefine extendClassDefine = clazz.getAnnotation(JsonTypeDefine.class);
                if (extendClassDefine == null) {
                    continue;
                }
                // Register the subtype and use the NamedType to build the relation
                objectMapper.registerSubtypes(new NamedType(clazz, extendClassDefine.value()));
            }
        }
    }

}

