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

/**
 * HexFunction
 * description: If the input argument is a numeric value (such as an integer), the HEX function converts the value to the corresponding hexadecimal string.
 *              If the input argument is a string, the HEX function converts each character in the string to its corresponding hexadecimal ASCII encoding and returns the hexadecimal representation of the entire string.
 */
public class HexFunction implements ValueParser {

    private ValueParser valueParser;

    public HexFunction(Function expr) {
        valueParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object valueObj = valueParser.parse(sourceData, rowIndex, context);
        try {
            return Integer.toHexString(OperatorTools.parseBigDecimal(valueObj).intValue()).toUpperCase();
        } catch (NumberFormatException e) {
            StringBuilder hexString = new StringBuilder();
            String str = OperatorTools.parseString(valueObj);
            for (char character : str.toCharArray()) {
                hexString.append(Integer.toHexString(character).toUpperCase());
            }
            return hexString.toString();
        }
    }
}
