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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;

/**
 * LcmFunction -> lcm(numeric_type,numeric_type)
 * description:
 * - return 0 if either input is zero
 * - return least common multiple (the smallest strictly positive number that is an integral multiple of both inputs)
 * Note: numeric_type includes floating-point number and integer
 */
@Slf4j
@TransformFunction(names = {"lcm"})
public class LcmFunction implements ValueParser {

    private final ValueParser firstNumParser;
    private final ValueParser secondNumTypeParser;

    public LcmFunction(Function expr) {
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
            return lcm(firstNum, secondNum).toPlainString();
        } catch (Exception e) {
            log.error("Parse error", e);
            return null;
        }
    }

    public static BigInteger gcd(BigDecimal a, BigDecimal b) {
        BigInteger intA = a.toBigInteger();
        BigInteger intB = b.toBigInteger();
        return intA.gcd(intB);
    }

    public static BigDecimal lcm(BigDecimal a, BigDecimal b) {
        if (a.compareTo(BigDecimal.ZERO) == 0 || b.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        int maxScale = Math.max(a.scale(), b.scale());

        BigDecimal factorA = a.movePointRight(maxScale);
        BigDecimal factorB = b.movePointRight(maxScale);

        BigInteger gcdValue = gcd(factorA, factorB);
        BigDecimal product = factorA.multiply(factorB);

        // LCM = (|a * b| / GCD(a, b))
        BigDecimal lcmValue = product.divide(new BigDecimal(gcdValue), RoundingMode.HALF_UP);

        return lcmValue.movePointLeft(maxScale).stripTrailingZeros();
    }

}