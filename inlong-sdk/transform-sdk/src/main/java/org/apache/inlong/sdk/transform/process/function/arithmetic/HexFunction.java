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
import java.math.RoundingMode;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * HexFunction  ->  hex(dataStr)
 * description:
 * - Return the string obtained by converting the dataStr to hexadecimal if 'dataStr' can be parsed into numeric
 * - Return the string obtained by converting the ASCII code corresponding to each character to hexadecimal otherwise
 */
@TransformFunction(type = FunctionConstant.ARITHMETIC_TYPE, names = {
        "hex"}, parameter = "(String dataStr)", descriptions = {
                "- Return \"\" if dataStr is NULL;",
                "- Return the string obtained by converting the dataStr to hexadecimal if 'dataStr' can be parsed into numeric;",
                "- Return the string obtained by converting the ASCII code corresponding to each character to hexadecimal otherwise."}, examples = {
                        "hex(1007) = \"3EF\"", "hex('abc') = \"616263\""})
public class HexFunction implements ValueParser {

    private static final Pattern BIG_DECIMAL_PATTERN = Pattern.compile("^[-+]?\\d+(\\.\\d+)?([eE][-+]?\\d+)?$");

    private ValueParser valueParser;

    public HexFunction(Function expr) {
        valueParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object valueObj = valueParser.parse(sourceData, rowIndex, context);
        if (isBigDecimal(valueObj)) {
            return hex(OperatorTools.parseBigDecimal(valueObj)).toUpperCase();
        }
        return hex(OperatorTools.parseString(valueObj)).toUpperCase();
    }

    private boolean isBigDecimal(Object valueObj) {
        if (valueObj instanceof BigDecimal) {
            return true;
        }
        if (valueObj instanceof String) {
            String str = (String) valueObj;
            return BIG_DECIMAL_PATTERN.matcher(str).matches();
        }
        return false;
    }

    // Handle Integer type
    private String hex(int number) {
        return Integer.toHexString(number).toUpperCase();
    }

    // Handle long type
    private String hex(long number) {
        return Long.toHexString(number).toUpperCase();
    }

    // Handle String type
    private String hex(String input) {
        StringBuilder hexString = new StringBuilder();
        for (char c : input.toCharArray()) {
            hexString.append(Integer.toHexString((int) c).toUpperCase());
        }
        return hexString.toString();
    }

    // Handle BigDecimal type
    private String hex(BigDecimal number) {
        // keep the integer part
        BigDecimal integerValue = number.setScale(0, RoundingMode.DOWN);
        return tryConvert(integerValue, BigDecimal::intValueExact, this::hex)
                // If it cannot convert to integer, try converting to long
                .orElseGet(() -> tryConvert(integerValue, BigDecimal::longValueExact, this::hex)
                        .orElseThrow(() -> new IllegalArgumentException("Number out of range")));
    }

    // Common conversion and processing methods
    private <T> Optional<String> tryConvert(BigDecimal number, java.util.function.Function<BigDecimal, T> converter,
            java.util.function.Function<T, String> handler) {
        try {
            T value = converter.apply(number);
            return Optional.ofNullable(handler.apply(value));
        } catch (ArithmeticException e) {
            return Optional.empty();
        }
    }
}
