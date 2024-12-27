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

package org.apache.inlong.sdk.transform.process.function.arithmetic;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;
import java.util.List;

/**
 * FactorialFunction  ->  factorial(numeric)
 * Description:
 * - Return NULL if 'numeric' is NULL;
 * - Return the factorial of a non-negative
 */
@TransformFunction(type = FunctionConstant.ARITHMETIC_TYPE, names = {
        "factorial"}, parameter = "(Numeric numeric)", descriptions = {
                "- Return \"\" if 'numeric' is NULL;",
                "- Return the factorial of a non-negative."
        }, examples = {
                "factorial(5) = 120",
                "factorial(0) = 1"
        })
@Slf4j
public class FactorialFunction implements ValueParser {

    private ValueParser numberParser;

    public FactorialFunction(Function expr) {
        if (expr.getParameters() != null) {
            List<Expression> expressions = expr.getParameters().getExpressions();
            if (expressions != null && expressions.size() == 1) {
                numberParser = OperatorTools.buildParser(expressions.get(0));
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (numberParser != null) {
            Object valueObj = numberParser.parse(sourceData, rowIndex, context);
            if (valueObj == null) {
                return null;
            }
            BigDecimal value = OperatorTools.parseBigDecimal(valueObj);
            if (value.scale() > 0 || value.compareTo(BigDecimal.ZERO) < 0) {
                log.warn("Factorial is only defined for non-negative integers. Invalid input: {}", value);
                return null;
            }
            return factorial(value.intValue());
        }
        return null;
    }

    private BigDecimal factorial(int n) {
        if (n < 0) {
            log.error("Factorial is not defined for negative numbers.");
            return null;
        }
        BigDecimal result = BigDecimal.ONE;
        for (int i = 2; i <= n; i++) {
            result = result.multiply(BigDecimal.valueOf(i));
        }
        return result;
    }
}
