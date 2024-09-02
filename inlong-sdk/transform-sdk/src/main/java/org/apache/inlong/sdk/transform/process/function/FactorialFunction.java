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

import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;

/**
 * FactorialFunction
 * description: factorial(numeric)--returns the factorial of a non-negative
 * integer
 */
public class FactorialFunction implements ValueParser {

    private ValueParser numberParser;

    /**
     * Constructor
     * 
     * @param expr
     */
    public FactorialFunction(Function expr) {
        if (expr == null || expr.getParameters() == null || expr.getParameters().getExpressions() == null
                || expr.getParameters().getExpressions().get(0) == null) {
            throw new IllegalArgumentException("Invalid expression for factorial function.");
        }
        numberParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
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
        Object numberObj = numberParser.parse(sourceData, rowIndex, context);
        if (numberObj == null) {
            throw new IllegalArgumentException("Number object cannot be null.");
        }
        BigDecimal numberValue = OperatorTools.parseBigDecimal(numberObj);

        System.out.println("Parsed number: " + numberValue);
        System.out.println("Scale: " + numberValue.scale());

        // Ensure the number is a non-negative integer
        if (numberValue.scale() > 0 || numberValue.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Factorial is only defined for non-negative integers.");
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
            throw new IllegalArgumentException("Factorial is not defined for negative numbers.");
        }
        BigDecimal result = BigDecimal.ONE;
        for (int i = 2; i <= n; i++) {
            result = result.multiply(BigDecimal.valueOf(i));
        }
        return result;
    }
}
