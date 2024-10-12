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

import java.nio.charset.Charset;
import java.util.List;

/**
 * LengthFunction  ->  length(string,[charset])
 * description:
 * - return NULL if the string is NULL
 * - return the byte length of the string
 * Note: charset defaults to matching with JVM.
 */
@TransformFunction(names = {"length"}, parameter = "(String str, String charset)", descriptions = {
        "- Return \"\" if 'str' is NULL;",
        "- Return the byte length of the 'str'.",
        "Note: charset defaults to matching with JVM."
}, examples = {
        "length(应龙, utf-8) = 6",
        "length('hello world') = 11"
})
public class LengthFunction implements ValueParser {

    private final ValueParser stringParser;
    private ValueParser charSetNameParser;
    private final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    public LengthFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        stringParser = OperatorTools.buildParser(expressions.get(0));
        if (expressions.size() > 1) {
            charSetNameParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObject = stringParser.parse(sourceData, rowIndex, context);
        if (stringObject == null) {
            return null;
        }
        Charset charset = DEFAULT_CHARSET;
        if (charSetNameParser != null) {
            charset = Charset.forName(OperatorTools.parseString(
                    charSetNameParser.parse(sourceData, rowIndex, context)));
        }
        String str = OperatorTools.parseString(stringObject);
        return str.getBytes(charset).length;
    }
}
