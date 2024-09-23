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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
/**
 * TruncateFunction
 * description: returns the number that intercepts integer2 decimal places.
 *              If numeric1 or integer2 is NULL, NULL is returned.
 *              If integer2 is 0, the result has no decimal point or fractional part.
 *              integer2 can be negative, making the integer2 digit to the left of the decimal point of the value zero.
 *              This function can also be used by passing only one numeric1 argument without setting Integer2.
 *              If Integer2 is not set, Integer2 is 0
 * for example: truncate(42.324, 2)--return 42.32
 *              truncate(42.324)--return 42.0
 */
@TransformFunction(names = {"truncate", "trunc"})
public class TruncateFunction implements ValueParser {

    private ValueParser bigDecimalParser;

    private ValueParser integerParser;

    public TruncateFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        if (expressions != null) {
            bigDecimalParser = OperatorTools.buildParser(expressions.get(0));
            if (expressions.size() >= 2) {
                integerParser = OperatorTools.buildParser(expressions.get(1));
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object bigDecimalObj = bigDecimalParser.parse(sourceData, rowIndex, context);
        BigDecimal bigDecimal = OperatorTools.parseBigDecimal(bigDecimalObj);
        if (integerParser != null) {
            Object integerObj = integerParser.parse(sourceData, rowIndex, context);
            int integer = OperatorTools.parseBigDecimal(integerObj).intValue();
            return truncate(bigDecimal, integer);
        }
        return truncate(bigDecimal);
    }

    private BigDecimal truncate(BigDecimal numeric1, Integer integer2) {
        if (numeric1 == null || integer2 == null) {
            return null;
        }
        if (integer2 < 0) {
            BigDecimal scaled = numeric1.movePointLeft(-integer2);
            BigDecimal truncated = scaled.setScale(0, RoundingMode.DOWN);
            return truncated.movePointRight(-integer2);
        }
        return numeric1.setScale(integer2, RoundingMode.DOWN);
    }

    private BigDecimal truncate(BigDecimal numeric1) {
        if (numeric1 == null) {
            return null;
        }
        return numeric1.setScale(0, RoundingMode.DOWN);
    }

}
