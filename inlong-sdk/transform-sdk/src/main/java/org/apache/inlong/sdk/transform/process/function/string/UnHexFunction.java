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
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Function;

/**
 * UnHexFunction  -> unhex(str)
 * description:
 * Return null if 'str' is null;
 * Return the result of interpreting each pair of characters in the argument as the character corresponding
 *        to its hexadecimal number.
 */
@TransformFunction(names = {"unhex"}, parameter = "(String str)", descriptions = {
        "- Return \"\" if 'str' is NULL;",
        "- Return the result of interpreting each pair of characters in the argument as the character" +
                " corresponding to its hexadecimal number.",
}, examples = {
        "unhex(\"696E6C6F6E67\") = \"inlong\""
})
public class UnHexFunction implements ValueParser {

    private ValueParser valueParser;

    public UnHexFunction(Function expr) {
        valueParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object valueObj = valueParser.parse(sourceData, rowIndex, context);
        if (valueObj == null) {
            return null;
        }
        return hexToString(OperatorTools.parseString(valueObj));
    }

    public static String hexToString(String hex) {
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < hex.length(); i += 2) {
            String str = hex.substring(i, i + 2);
            char ch = (char) Integer.parseInt(str, 16);
            output.append(ch);
        }
        return output.toString();
    }
}
