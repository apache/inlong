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

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.lang.ObjectUtils;
import org.apache.inlong.sdk.transform.process.function.*;
import org.apache.inlong.sdk.transform.process.parser.*;

import java.math.BigDecimal;
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
        } else if (expr instanceof Function) {
            String exprString = expr.toString();
            if (exprString.startsWith(ROOT_KEY) || exprString.startsWith(CHILD_KEY)) {
                return new ColumnParser((Function) expr);
            } else {
                // TODO
                Function func = (Function) expr;
                java.util.function.Function<Function, ValueParser> valueParserConstructor = functionMap.get(func.getName().toLowerCase());
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
