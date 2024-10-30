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

import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;

/**
 * Atan2dFunction  ->  atan2d(numericx,numericy)
 * description:
 * - Return NULL if 'numericx' or 'numericy' is NULL;
 * - Return inverse tangent of 'numericy'/'numericx', result in degrees.
 */
@TransformFunction(type = FunctionConstant.ARITHMETIC_TYPE, names = {
        "atan2d"}, parameter = "(Numeric numericx, Numeric numericy)", descriptions = {
                "- Return \"\" if 'numericx' or 'numericy' is NULL;",
                "- Return inverse tangent of 'numericy'/'numericx', result in degrees."
        }, examples = {
                "atan2d(1, 1) = 45.0",
                "atan2d(1, 0) = 90.0",
                "atan2d(0, -1) = 180.0"
        })
public class Atan2dFunction implements ValueParser {

    private ValueParser xParser;
    private ValueParser yParser;

    public Atan2dFunction(Function expr) {
        xParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
        yParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object xObj = xParser.parse(sourceData, rowIndex, context);
        Object yObj = yParser.parse(sourceData, rowIndex, context);

        if (xObj == null || yObj == null) {
            return null;
        }

        BigDecimal xValue = OperatorTools.parseBigDecimal(xObj);
        BigDecimal yValue = OperatorTools.parseBigDecimal(yObj);

        return Math.toDegrees(Math.atan2(xValue.doubleValue(), yValue.doubleValue()));
    }
}
