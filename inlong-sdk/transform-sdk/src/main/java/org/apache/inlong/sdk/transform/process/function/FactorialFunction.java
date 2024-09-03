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

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;

/**
 * FactorialFunction
 * description: factorial(numeric)--returns the factorial of a non-negative
 * integer
 */
@Slf4j
public class FactorialFunction implements ValueParser {

    private final ValueParser numberParser;

    /**
     * Constructor
     *
     * @param expr
     */
    public FactorialFunction(Function expr) {
        if (expr == null || expr.getParameters() == null || expr.getParameters().getExpressions() == null
                || expr.getParameters().getExpressions().isEmpty()
                || expr.getParameters().getExpressions().get(0) == null) {
            log.error("Invalid expression for factorial function.");
            numberParser = null;
        } else {
            numberParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
        }
    }

    /**
     * parse
     *
     * @param sourceData
     * @param rowIndex
     * @return
     */
    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (numberParser == null) {
            log.error("Number parser is not initialized.");
            return null;
        }

        Object numberObj;
        try {
            numberObj = numberParser.parse(sourceData, rowIndex, context);
        } catch (Exception e) {
            log.error("Error parsing number object", e);
            return null;
        }

        if (numberObj == null) {
            log.warn("Parsed number object is null.");
            return null;
        }

        BigDecimal numberValue;
        try {
            numberValue = OperatorTools.parseBigDecimal(numberObj);
        } catch (Exception e) {
            log.error("Error converting parsed object to BigDecimal", e);
            return null;
        }

        // Ensure the number is a non-negative integer
        if (numberValue.scale() > 0 || numberValue.compareTo(BigDecimal.ZERO) < 0) {
            log.warn("Factorial is only defined for non-negative integers. Invalid input: {}", numberValue);
            return null;
        }

        return factorial(numberValue.intValue());
    }

    /**
     * Helper method to calculate factorial
     *
     * @param n
     * @return
     */
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
