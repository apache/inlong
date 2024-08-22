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
import org.apache.inlong.sdk.transform.process.function.FloorFunction;
import org.apache.inlong.sdk.transform.process.function.FromUnixTimeFunction;
import org.apache.inlong.sdk.transform.process.function.LnFunction;
import org.apache.inlong.sdk.transform.process.function.LocateFunction;
import org.apache.inlong.sdk.transform.process.function.Log10Function;
import org.apache.inlong.sdk.transform.process.function.Log2Function;
import org.apache.inlong.sdk.transform.process.function.LogFunction;
import org.apache.inlong.sdk.transform.process.function.NowFunction;
import org.apache.inlong.sdk.transform.process.function.PowerFunction;
import org.apache.inlong.sdk.transform.process.function.ReplicateFunction;
import org.apache.inlong.sdk.transform.process.function.RoundFunction;
import org.apache.inlong.sdk.transform.process.function.SinFunction;
import org.apache.inlong.sdk.transform.process.function.SinhFunction;
import org.apache.inlong.sdk.transform.process.function.SqrtFunction;
import org.apache.inlong.sdk.transform.process.function.SubstringFunction;
import org.apache.inlong.sdk.transform.process.function.TanFunction;
import org.apache.inlong.sdk.transform.process.function.TimestampExtractFunction;
import org.apache.inlong.sdk.transform.process.function.ToBase64Function;
import org.apache.inlong.sdk.transform.process.function.ToDateFunction;
import org.apache.inlong.sdk.transform.process.function.ToTimestampFunction;
import org.apache.inlong.sdk.transform.process.function.TrimFunction;
import org.apache.inlong.sdk.transform.process.function.UnixTimestampFunction;
import org.apache.inlong.sdk.transform.process.parser.AdditionParser;
import org.apache.inlong.sdk.transform.process.parser.ColumnParser;
import org.apache.inlong.sdk.transform.process.parser.DateParser;
import org.apache.inlong.sdk.transform.process.parser.DivisionParser;
import org.apache.inlong.sdk.transform.process.parser.LongParser;
import org.apache.inlong.sdk.transform.process.parser.MultiplicationParser;
import org.apache.inlong.sdk.transform.process.parser.ParenthesisParser;
import org.apache.inlong.sdk.transform.process.parser.StringParser;
import org.apache.inlong.sdk.transform.process.parser.SubtractionParser;
import org.apache.inlong.sdk.transform.process.parser.TimestampParser;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.lang.ObjectUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * OperatorTools
 * 
 */
public class OperatorTools {

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
        functionMap.put("trim", TrimFunction::new);
        functionMap.put("replicate", ReplicateFunction::new);
        functionMap.put("locate", LocateFunction::new);
        functionMap.put("to_date", ToDateFunction::new);
        functionMap.put("date_format", DateFormatFunction::new);
        functionMap.put("ceil", CeilFunction::new);
        functionMap.put("floor", FloorFunction::new);
        functionMap.put("sin", SinFunction::new);
        functionMap.put("sinh", SinhFunction::new);
        functionMap.put("cos", CosFunction::new);
        functionMap.put("tan", TanFunction::new);
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
        functionMap.put("to_base64", ToBase64Function::new);
    }

    public static ExpressionOperator buildOperator(Expression expr) {
        if (expr instanceof AndExpression) {
            return new AndOperator((AndExpression) expr);
        } else if (expr instanceof OrExpression) {
            return new OrOperator((OrExpression) expr);
        } else if (expr instanceof Parenthesis) {
            return new ParenthesisOperator((Parenthesis) expr);
        } else if (expr instanceof NotExpression) {
            return new NotOperator((NotExpression) expr);
        } else if (expr instanceof EqualsTo) {
            return new EqualsToOperator((EqualsTo) expr);
        } else if (expr instanceof NotEqualsTo) {
            return new NotEqualsToOperator((NotEqualsTo) expr);
        } else if (expr instanceof GreaterThan) {
            return new GreaterThanOperator((GreaterThan) expr);
        } else if (expr instanceof GreaterThanEquals) {
            return new GreaterThanEqualsOperator((GreaterThanEquals) expr);
        } else if (expr instanceof MinorThan) {
            return new MinorThanOperator((MinorThan) expr);
        } else if (expr instanceof MinorThanEquals) {
            return new MinorThanEqualsOperator((MinorThanEquals) expr);
        }
        return null;
    }

    public static ValueParser buildParser(Expression expr) {
        if (expr instanceof Column) {
            return new ColumnParser((Column) expr);
        } else if (expr instanceof StringValue) {
            return new StringParser((StringValue) expr);
        } else if (expr instanceof LongValue) {
            return new LongParser((LongValue) expr);
        } else if (expr instanceof Parenthesis) {
            return new ParenthesisParser((Parenthesis) expr);
        } else if (expr instanceof Addition) {
            return new AdditionParser((Addition) expr);
        } else if (expr instanceof Subtraction) {
            return new SubtractionParser((Subtraction) expr);
        } else if (expr instanceof Multiplication) {
            return new MultiplicationParser((Multiplication) expr);
        } else if (expr instanceof Division) {
            return new DivisionParser((Division) expr);
        } else if (expr instanceof DateValue) {
            return new DateParser((DateValue) expr);
        } else if (expr instanceof TimestampValue) {
            return new TimestampParser((TimestampValue) expr);
        } else if (expr instanceof Function) {
            String exprString = expr.toString();
            if (exprString.startsWith(ROOT_KEY) || exprString.startsWith(CHILD_KEY)) {
                return new ColumnParser((Function) expr);
            } else {
                // TODO
                Function func = (Function) expr;
                java.util.function.Function<Function, ValueParser> valueParserConstructor =
                        functionMap.get(func.getName().toLowerCase());
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
        if (left instanceof String) {
            if (right instanceof String) {
                return ObjectUtils.compare(left, right);
            } else {
                BigDecimal leftValue = parseBigDecimal(left);
                return ObjectUtils.compare(leftValue, right);
            }
        } else {
            if (right instanceof String) {
                BigDecimal rightValue = parseBigDecimal(right);
                return ObjectUtils.compare(left, rightValue);
            } else {
                return ObjectUtils.compare(left, right);
            }
        }
    }
}
