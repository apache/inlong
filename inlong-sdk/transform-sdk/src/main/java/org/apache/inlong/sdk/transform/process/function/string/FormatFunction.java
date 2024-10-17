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

package org.apache.inlong.sdk.transform.process.function.string;

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
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

/**
 * FormatFunction  ->  FORMAT(X,D)
 * description:
 * return NULL if X or D is NULL.
 * return the result of formatting the number X to "#,###,###.##" format, rounded to D decimal places.
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "format"}, parameter = "(Numeric X,Integer D)", descriptions = {
                "- Return \"\" if 'X' or 'D' is NULL;",
                "- Return the result of formatting the number 'X' to \"#,###,###.##\" format, rounded to 'D' decimal places."
        }, examples = {
                "FORMAT(12332.123456, 4) = \"12,332.1235\"",
                "FORMAT(12332.2,0) = \"12,332\""
        })
public class FormatFunction implements ValueParser {

    private ValueParser numberParser;
    private ValueParser reservedDigitsParser;

    public FormatFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        numberParser = OperatorTools.buildParser(expressions.get(0));
        reservedDigitsParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numberObj = numberParser.parse(sourceData, rowIndex, context);
        Object reservedDigitsObj = reservedDigitsParser.parse(sourceData, rowIndex, context);
        if (numberObj == null || reservedDigitsObj == null) {
            return null;
        }
        BigDecimal number = OperatorTools.parseBigDecimal(numberObj);
        int reservedDigits = OperatorTools.parseBigDecimal(reservedDigitsObj).intValue();
        if (reservedDigits < 0) {
            reservedDigits = 0;
        }
        // build format
        StringBuilder pattern = new StringBuilder("#,###");
        if (reservedDigits > 0) {
            pattern.append(".");
            for (int i = 0; i < reservedDigits; i++) {
                pattern.append("0");
            }
        }
        number = number.setScale(reservedDigits, RoundingMode.HALF_UP);
        DecimalFormat df = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
        df.applyPattern(pattern.toString());
        return df.format(number);
    }
}
