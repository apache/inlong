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

package org.apache.inlong.sdk.transform.process.function;

import org.apache.inlong.sdk.transform.process.parser.ColumnParser;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;
import org.apache.inlong.sdk.transform.process.pojo.FunctionInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Function;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class FunctionTools {

    private static final String FUNCTION_PATH = "org.apache.inlong.sdk.transform.process.function";
    private final static Map<String, Class<?>> functionMap = Maps.newConcurrentMap();

    static {
        init();
    }

    private static void init() {
        Reflections reflections = new Reflections(FUNCTION_PATH, Scanners.TypesAnnotated);
        Set<Class<?>> clazzSet = reflections.getTypesAnnotatedWith(TransformFunction.class);
        for (Class<?> clazz : clazzSet) {
            TransformFunction annotation = clazz.getAnnotation(TransformFunction.class);
            if (annotation == null || ArrayUtils.isEmpty(annotation.names())) {
                continue;
            }

            String[] functionNames = annotation.names();
            for (String functionName : functionNames) {
                if (StringUtils.isEmpty(functionName)) {
                    continue;
                }

                functionMap.compute(functionName, (name, former) -> {
                    if (former != null) {
                        log.warn("find a conflict function named [{}], the former one is [{}], new one is [{}]",
                                name, former.getName(), clazz.getName());
                    }
                    return clazz;
                });
            }
        }
    }

    private static class FunctionDocHolder {

        final static List<FunctionInfo> functionDocList = Lists.newCopyOnWriteArrayList();

        static {
            initFunctionDoc();
        }

        private static void initFunctionDoc() {
            Collection<Class<?>> clazzList = functionMap.values();
            for (Class<?> clazz : clazzList) {
                TransformFunction annotation = clazz.getAnnotation(TransformFunction.class);
                if (annotation == null || ArrayUtils.isEmpty(annotation.names())) {
                    continue;
                }
                StringBuilder name = new StringBuilder();
                StringBuilder explanation = new StringBuilder();
                StringBuilder example = new StringBuilder();
                for (String functionName : annotation.names()) {
                    name.append(functionName.concat(annotation.parameter() + "\r\n"));
                }
                for (String functionExplanation : annotation.descriptions()) {
                    explanation.append(functionExplanation.concat("\r\n"));
                }
                for (String functionExample : annotation.examples()) {
                    example.append(functionExample.concat("\r\n"));
                }
                functionDocList.add(new FunctionInfo(name.toString(), explanation.toString(), example.toString()));
            }
        }
    }

    public static List<FunctionInfo> getFunctionDoc() {
        return FunctionDocHolder.functionDocList;
    }

    public static ValueParser getTransformFunction(Function func) {
        if (func == null) {
            return null;
        }
        String functionName = func.getName().toLowerCase();
        Class<?> clazz = functionMap.get(functionName);
        if (clazz == null) {
            return new ColumnParser(func);
        }
        try {
            Constructor<?> constructor = clazz.getDeclaredConstructor(func.getClass());
            return (ValueParser) constructor.newInstance(func);
        } catch (NoSuchMethodException e) {
            log.error("transform function {} needs one constructor that accept one params whose type is {}",
                    clazz.getName(), func.getClass().getName(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
