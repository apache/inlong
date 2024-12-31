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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;
import java.util.List;

/**
 * FibonacciFunction  ->  fibonacci(numeric)
 * Description:
 * - Return NULL if 'numeric' is NULL;
 * - Returns the nth Fibonacci number
 */
@TransformFunction(type = FunctionConstant.ARITHMETIC_TYPE, names = {
        "fibonacci"}, parameter = "(Numeric numeric)", descriptions = {
                "- Return NULL if 'numeric' is NULL;",
                "- Returns the nth Fibonacci number"
        }, examples = {"fibonacci(0) = 0", "fibonacci(1) = 1", "fibonacci(2) = 1", "fibonacci(3) = 2",
                "fibonacci(4) = 3"})
public class FibonacciFunction implements ValueParser {

    private ValueParser numberParser;

    public FibonacciFunction(Function expr) {
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
            BigDecimal numberValue = OperatorTools.parseBigDecimal(valueObj);
            return fibonacci(numberValue.intValue());
        }
        return null;
    }

    private long fibonacci(int n) {
        if (n <= 1) {
            return n;
        }
        long prev = 0, curr = 1;
        for (int i = 2; i <= n; i++) {
            long temp = curr;
            curr = curr + prev;
            prev = temp;
        }
        return curr;
    }
}
