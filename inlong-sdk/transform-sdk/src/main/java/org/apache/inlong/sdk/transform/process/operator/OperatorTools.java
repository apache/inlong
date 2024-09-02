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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import org.apache.commons.lang.ObjectUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * OperatorTools
 *
 */
public class OperatorTools {

    public static final String ROOT_KEY = "$root";

    public static final String CHILD_KEY = "$child";
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
