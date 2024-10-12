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
import java.math.RoundingMode;
import java.util.List;

/**
 * RoundFunction  ->  ROUND(x [,y])
 * description:
 * - Return NULL if 'x' is NULL
 * - Return the nearest integer to 'x', with optional parameter 'y' indicating the number of decimal places to be rounded
 */
@TransformFunction(type = FunctionConstant.ARITHMETIC_TYPE, names = {
        "round"}, parameter = "(String str)", descriptions = {
                "- Return \"\" if 'x' is NULL;",
                "- Return the nearest integer to 'x', with optional parameter 'y' indicating the number of decimal "
                        + "places to be rounded."}, examples = {"round(3.5) = 4",
                                "round(3.14159265358979323846,10) = 3.1415926536"})
public class RoundFunction implements ValueParser {

    private ValueParser numberParser;
    private ValueParser reservedDigitsParser;

    public RoundFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        numberParser = OperatorTools.buildParser(expressions.get(0));
        if (expressions.size() == 2) {
            reservedDigitsParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numberObj = numberParser.parse(sourceData, rowIndex, context);
        if (numberObj == null) {
            return null;
        }
        BigDecimal number = OperatorTools.parseBigDecimal(numberObj);
        if (reservedDigitsParser != null) {
            Object reservedDigitsObj = reservedDigitsParser.parse(sourceData, rowIndex, context);
            int reservedDigits = OperatorTools.parseBigDecimal(reservedDigitsObj).intValue();
            return number.setScale(reservedDigits, RoundingMode.HALF_UP).doubleValue();
        }
        return Math.round(number.doubleValue());
    }
}
