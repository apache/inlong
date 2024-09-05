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

import org.apache.inlong.sdk.transform.process.function.AbsFunction;
import org.apache.inlong.sdk.transform.process.function.CeilFunction;
import org.apache.inlong.sdk.transform.process.function.ConcatFunction;
import org.apache.inlong.sdk.transform.process.function.CosFunction;
import org.apache.inlong.sdk.transform.process.function.DateExtractFunction;
import org.apache.inlong.sdk.transform.process.function.DateExtractFunction.DateExtractFunctionType;
import org.apache.inlong.sdk.transform.process.function.DateFormatFunction;
import org.apache.inlong.sdk.transform.process.function.ExpFunction;
import org.apache.inlong.sdk.transform.process.function.FibonacciFunction;
import org.apache.inlong.sdk.transform.process.function.FloorFunction;
import org.apache.inlong.sdk.transform.process.function.FromUnixTimeFunction;
import org.apache.inlong.sdk.transform.process.function.LnFunction;
import org.apache.inlong.sdk.transform.process.function.LocateFunction;
import org.apache.inlong.sdk.transform.process.function.Log10Function;
import org.apache.inlong.sdk.transform.process.function.Log2Function;
import org.apache.inlong.sdk.transform.process.function.LogFunction;
import org.apache.inlong.sdk.transform.process.function.NowFunction;
import org.apache.inlong.sdk.transform.process.function.PowerFunction;
import org.apache.inlong.sdk.transform.process.function.RoundFunction;
import org.apache.inlong.sdk.transform.process.function.SinFunction;
import org.apache.inlong.sdk.transform.process.function.SinhFunction;
import org.apache.inlong.sdk.transform.process.function.SqrtFunction;
import org.apache.inlong.sdk.transform.process.function.SubstringFunction;
import org.apache.inlong.sdk.transform.process.function.TimestampExtractFunction;
import org.apache.inlong.sdk.transform.process.function.ToDateFunction;
import org.apache.inlong.sdk.transform.process.function.ToTimestampFunction;
import org.apache.inlong.sdk.transform.process.function.UnixTimestampFunction;
import org.apache.inlong.sdk.transform.process.parser.ColumnParser;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.commons.lang.ObjectUtils;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

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

    private static final Map<String, java.util.function.Function<Function, ValueParser>> functionMap = new HashMap<>();

    static {
        functionMap.put("concat", ConcatFunction::new);
        functionMap.put("now", NowFunction::new);
        functionMap.put("power", PowerFunction::new);
        functionMap.put("abs", AbsFunction::new);
        functionMap.put("sqrt", SqrtFunction::new);
        functionMap.put("ln", LnFunction::new);
        functionMap.put("log10", Log10Function::new);
        functionMap.put("log2", Log2Function::new);
        functionMap.put("log", LogFunction::new);
        functionMap.put("exp", ExpFunction::new);
        functionMap.put("substring", SubstringFunction::new);
        functionMap.put("locate", LocateFunction::new);
        functionMap.put("to_date", ToDateFunction::new);
        functionMap.put("date_format", DateFormatFunction::new);
        functionMap.put("ceil", CeilFunction::new);
        functionMap.put("floor", FloorFunction::new);
        functionMap.put("sin", SinFunction::new);
        functionMap.put("sinh", SinhFunction::new);
        functionMap.put("cos", CosFunction::new);
        functionMap.put("year", func -> new DateExtractFunction(DateExtractFunctionType.YEAR, func));
        functionMap.put("quarter", func -> new DateExtractFunction(DateExtractFunctionType.QUARTER, func));
        functionMap.put("month", func -> new DateExtractFunction(DateExtractFunctionType.MONTH, func));
        functionMap.put("week", func -> new DateExtractFunction(DateExtractFunctionType.WEEK, func));
        functionMap.put("dayofyear", func -> new DateExtractFunction(DateExtractFunctionType.DAY_OF_YEAR, func));
        functionMap.put("dayofmonth", func -> new DateExtractFunction(DateExtractFunctionType.DAY_OF_MONTH, func));
        functionMap.put("hour",
                func -> new TimestampExtractFunction(TimestampExtractFunction.TimestampExtractFunctionType.HOUR, func));
        functionMap.put("minute",
                func -> new TimestampExtractFunction(TimestampExtractFunction.TimestampExtractFunctionType.MINUTE,
                        func));
        functionMap.put("second",
                func -> new TimestampExtractFunction(TimestampExtractFunction.TimestampExtractFunctionType.SECOND,
                        func));
        functionMap.put("round", RoundFunction::new);
        functionMap.put("from_unixtime", FromUnixTimeFunction::new);
        functionMap.put("unix_timestamp", UnixTimestampFunction::new);
        functionMap.put("to_timestamp", ToTimestampFunction::new);
        functionMap.put("fibonacci", FibonacciFunction::new);

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
                // TODO
                Function func = (Function) expr;
                java.util.function.Function<Function, ValueParser> valueParserConstructor = functionMap
                        .get(func.getName().toLowerCase());
                if (valueParserConstructor != null) {
                    return valueParserConstructor.apply(func);
                } else {
                    return new ColumnParser(func);
                }
            }
        }
        return null;
    }

    /**
     * parseBigDecimal
     * 
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

    /**
     * compareValue
     * 
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
