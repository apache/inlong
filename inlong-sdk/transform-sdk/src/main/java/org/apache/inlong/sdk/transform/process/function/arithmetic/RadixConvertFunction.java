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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.math.BigInteger;
import java.util.List;

/**
 * RadixConvertFunction  ->  radix_convert(numeric,from_base,to_base)
 * description:
 * - Return NULL if any of its arguments are NULL
 * - Return the result of converting 'numeric' from 'from_base' to 'to_base'
 * Note: abs(base) between [2,36].'from_base' is a negative number, 'numeric' is regarded as a signed number.
 *       Otherwise, 'numeric' is treated as unsigned. This function works with 64-bit precision.
 */
@TransformFunction(names = {"radix_convert"}, parameter = "(Numeric numeric)", descriptions = {
        "- Return \"\" if any of its arguments are NULL;",
        "- Return the result of converting 'numeric' from 'from_base' to 'to_base'.",
        "Note: abs(base) between [2,36].'from_base' is a negative number, 'numeric' is regarded as a signed number." +
                "Otherwise, 'numeric' is treated as unsigned. This function works with 64-bit precision."
}, examples = {
        "radix_convert('6E',18,8) = 172"
})
public class RadixConvertFunction implements ValueParser {

    private final ValueParser numParser;
    private final ValueParser fromBaseParser;
    private final ValueParser toBaseParser;

    public RadixConvertFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        numParser = OperatorTools.buildParser(expressions.get(0));
        fromBaseParser = OperatorTools.buildParser(expressions.get(1));
        toBaseParser = OperatorTools.buildParser(expressions.get(2));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numObj = numParser.parse(sourceData, rowIndex, context);
        Object fromBaseObj = fromBaseParser.parse(sourceData, rowIndex, context);
        Object toBaseObj = toBaseParser.parse(sourceData, rowIndex, context);
        if (numObj == null || fromBaseObj == null || toBaseObj == null) {
            return null;
        }
        String num = OperatorTools.parseString(numObj);
        int fromBase = Integer.parseInt(fromBaseObj.toString());
        int toBase = Integer.parseInt(toBaseObj.toString());
        return conv(num, fromBase, toBase);
    }

    /**
     * Converts a number from one base to another.
     *
     * @param number   The number to be converted (as a string or integer).
     * @param fromBase The base of the input number.
     * @param toBase   The base to which the number should be converted.
     * @return The converted number as a string, or null if input is invalid.
     */
    public static String conv(String number, int fromBase, int toBase) {
        if (!checkRange(fromBase) || !checkRange(toBase)) {
            return null;
        }
        try {
            BigInteger num = new BigInteger(number, Math.abs(fromBase));
            if (fromBase > 0) {
                return num.toString(Math.abs(toBase)).toUpperCase();
            } else {
                return new BigInteger(Long.toUnsignedString(num.longValue()), 10)
                        .toString(Math.abs(toBase)).toUpperCase();
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static boolean checkRange(int base) {
        return Math.abs(base) >= 2 && Math.abs(base) <= 36;
    }
}
