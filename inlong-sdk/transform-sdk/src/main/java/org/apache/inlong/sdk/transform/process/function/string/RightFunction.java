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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

/**
 * RightFunction  ->  right(str,len)
 * description:
 * - Return NULL if either 'str' or 'len' is NULL
 * - Return "" if it is less than or equal to zero
 * - Return a substring of 'len' starting from the right side of the 'str'.
 */
@TransformFunction(names = {"right"}, parameter = "(String str)", descriptions = {
        "- Return \"\" if either 'str' or 'len' is NULL;",
        "- Return \"\" if it is less than or equal to zero;",
        "- Return a substring of 'len' starting from the right side of the 'str'."
}, examples = {
        "right('hello world',100) = \"hello world\""
})
public class RightFunction implements ValueParser {

    private final ValueParser stringParser;
    private final ValueParser lengthParser;

    public RightFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        stringParser = OperatorTools.buildParser(expressions.get(0));
        lengthParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObj = stringParser.parse(sourceData, rowIndex, context);
        Object lengthObj = lengthParser.parse(sourceData, rowIndex, context);
        if (stringObj == null || lengthObj == null) {
            return null;
        }
        String str = OperatorTools.parseString(stringObj);
        int len = Integer.parseInt(OperatorTools.parseString(lengthObj));
        if (len <= 0) {
            return "";
        }
        return str.substring(Math.max(str.length() - len, 0));
    }
}
