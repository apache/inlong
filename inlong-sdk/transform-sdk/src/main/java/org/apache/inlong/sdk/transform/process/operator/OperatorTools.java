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

package org.apache.inlong.sdk.transform.process.operator;

import org.apache.inlong.sdk.transform.process.function.FunctionTools;
import org.apache.inlong.sdk.transform.process.parser.ColumnParser;
import org.apache.inlong.sdk.transform.process.parser.ParserTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.commons.lang.ObjectUtils;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;

/**
 * OperatorTools
 *
 */
@Slf4j
public class OperatorTools {

    private static final String OPERATOR_PATH = "org.apache.inlong.sdk.transform.process.operator";

    private final static Map<Class<?>, Class<?>> operatorMap = Maps.newConcurrentMap();

    public static final String ROOT_KEY = "$root";

    public static final String CHILD_KEY = "$child";

    static {
        init();
    }

    private static void init() {
        Reflections reflections = new Reflections(OPERATOR_PATH,
                new TypeAnnotationsScanner(),
                new SubTypesScanner());
        Set<Class<?>> clazzSet = reflections.getTypesAnnotatedWith(TransformOperator.class);
        for (Class<?> clazz : clazzSet) {
            if (ExpressionOperator.class.isAssignableFrom(clazz)) {
                TransformOperator annotation = clazz.getAnnotation(TransformOperator.class);
                if (annotation == null) {
                    continue;
                }
                Class<?>[] values = annotation.values();
                for (Class<?> value : values) {
                    operatorMap.compute(value, (key, former) -> {
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

    public static ExpressionOperator getTransformOperator(Expression expr) {
        Class<?> clazz = operatorMap.get(expr.getClass());
        if (clazz == null) {
            return null;
        }
        try {
            Constructor<?> constructor = clazz.getDeclaredConstructor(expr.getClass());
            return (ExpressionOperator) constructor.newInstance(expr);
        } catch (NoSuchMethodException e) {
            log.error("transform operator {} needs one constructor that accept one params whose type is {}",
                    clazz.getName(), expr.getClass().getName(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ExpressionOperator buildOperator(Expression expr) {
        if (expr != null) {
            return getTransformOperator(expr);
        }
        return null;
    }

    public static ValueParser buildParser(Expression expr) {
        if (expr instanceof Function) {
            String exprString = expr.toString();
            if (exprString.startsWith(ROOT_KEY) || exprString.startsWith(CHILD_KEY)) {
                return new ColumnParser((Function) expr);
            } else {
                return FunctionTools.getTransformFunction((Function) expr);
            }
        }
        return ParserTools.getTransformParser(expr);
    }

    /**
     * parseBigDecimal
     * @param value
     * @return
     */
    public static BigDecimal parseBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else {
            return new BigDecimal(String.valueOf(value));
        }
    }

    public static String parseString(Object value) {
        return value.toString();
    }

    public static Date parseDate(Object value) {
        if (value instanceof Date) {
            return (Date) value;
        } else {
            return Date.valueOf(String.valueOf(value));
        }
    }

    public static Timestamp parseTimestamp(Object value) {
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        } else {
            return Timestamp.valueOf(String.valueOf(value));
        }
    }

    public static byte[] parseBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        } else {
            return (String.valueOf(value)).getBytes(StandardCharsets.UTF_8);
        }
    }

    public static boolean parseBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else {
            return Boolean.parseBoolean(String.valueOf(value));
        }
    }

    /**
     * compareValue
     * @param left
     * @param right
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static int compareValue(Comparable left, Comparable right) {
        if (left == null) {
            return right == null ? 0 : -1;
        }
        if (right == null) {
            return 1;
        }

        if (((Object) left).getClass() == ((Object) right).getClass()) {
            return ObjectUtils.compare(left, right);
        } else {
            try {
                BigDecimal leftValue = parseBigDecimal(left);
                BigDecimal rightValue = parseBigDecimal(right);
                return ObjectUtils.compare(leftValue, rightValue);
            } catch (Exception e) {
                String leftValue = parseString(left);
                String rightValue = parseString(right);
                return ObjectUtils.compare(leftValue, rightValue);
            }
        }
    }
}
