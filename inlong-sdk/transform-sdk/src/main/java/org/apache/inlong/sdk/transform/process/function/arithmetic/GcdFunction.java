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
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;
import java.util.List;

/**
 * GcdFunction -> gcd(numeric_type,numeric_type)
 * description:
 * - Return NULL if any parameter is null
 * - Return 0 if both inputs are zero
 * - Return greatest common divisor (the largest positive number that divides both inputs with no remainder).
 */
@Slf4j
@TransformFunction(names = {"gcd"}, parameter = "(Numeric numeric1,Numeric numeric2)", descriptions = {
        "- Return \"\" if any parameter is NULL;",
        "- Return 0 if both inputs are zero;",
        "- Return greatest common divisor (the largest positive number that divides both inputs with no remainder).",
}, examples = {
        "gcd(3.141,3.846) = 0.003"
})
public class GcdFunction implements ValueParser {

    private final ValueParser firstNumParser;
    private final ValueParser secondNumTypeParser;

    public GcdFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        firstNumParser = OperatorTools.buildParser(expressions.get(0));
        secondNumTypeParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object firstNumObj = firstNumParser.parse(sourceData, rowIndex, context);
        Object secondNumObj = secondNumTypeParser.parse(sourceData, rowIndex, context);
        if (firstNumObj == null || secondNumObj == null) {
            return null;
        }
        try {
            BigDecimal firstNum = OperatorTools.parseBigDecimal(firstNumObj);
            BigDecimal secondNum = OperatorTools.parseBigDecimal(secondNumObj);
            return gcdForBigDecimals(firstNum, secondNum);
        } catch (Exception e) {
            log.error("Parse error", e);
            return null;
        }
    }

    public static BigDecimal gcd(BigDecimal a, BigDecimal b) {
        if (b.compareTo(BigDecimal.ZERO) == 0) {
            return a;
        }
        BigDecimal remainder = a.remainder(b);
        return gcd(b, remainder);
    }

    /**
     * Support floating-point and integer gcd
     *
     * @param a first number
     * @param b second number
     * @return The greatest common divisor of a and b
     */
    public static BigDecimal gcdForBigDecimals(BigDecimal a, BigDecimal b) {
        int scaleA = a.scale();
        int scaleB = b.scale();

        BigDecimal scaledA = a.movePointRight(Math.max(scaleA, scaleB));
        BigDecimal scaledB = b.movePointRight(Math.max(scaleA, scaleB));

        BigDecimal gcdValue = gcd(scaledA, scaledB);

        return gcdValue.movePointLeft(Math.max(scaleA, scaleB));
    }
}