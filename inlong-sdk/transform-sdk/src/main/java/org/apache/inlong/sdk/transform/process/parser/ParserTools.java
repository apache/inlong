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

package org.apache.inlong.sdk.transform.process.parser;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Set;

@Slf4j
public class ParserTools {

    private static final String PARSER_PATH = "org.apache.inlong.sdk.transform.process.parser";
    private final static Map<Class<? extends Expression>, Class<?>> parserMap = Maps.newConcurrentMap();

    static {
        init();
    }
    private static void init() {
        Reflections reflections = new Reflections(PARSER_PATH,
                new TypeAnnotationsScanner(),
                new SubTypesScanner());
        Set<Class<?>> clazzSet = reflections.getTypesAnnotatedWith(TransformParser.class);
        for (Class<?> clazz : clazzSet) {
            if (ValueParser.class.isAssignableFrom(clazz)) {
                TransformParser annotation = clazz.getAnnotation(TransformParser.class);
                if (annotation == null) {
                    continue;
                }
                Class<? extends Expression>[] values = annotation.values();
                for (Class<? extends Expression> value : values) {
                    parserMap.compute(value, (key, former) -> {
                        if (former != null) {
                            log.warn("find a conflict for parser class [{}], the former one is [{}], new one is [{}]",
                                    key, former.getName(), clazz.getName());
                        }
                        return clazz;
                    });
                }
            }
        }
    }

    public static ValueParser getTransformParser(Expression expr) {
        if (expr == null) {
            return null;
        }
        Class<?> clazz = parserMap.get(expr.getClass());
        if (clazz == null) {
            return new ColumnParser((Column) expr);
        }
        try {
            Constructor<?> constructor = clazz.getDeclaredConstructor(expr.getClass());
            return (ValueParser) constructor.newInstance(expr);
        } catch (NoSuchMethodException e) {
            log.error("transform parser {} needs one constructor that accept one params whose type is {}",
                    clazz.getName(), expr.getClass().getName(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
