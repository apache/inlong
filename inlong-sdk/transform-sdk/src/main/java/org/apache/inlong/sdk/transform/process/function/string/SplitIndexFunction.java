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
 * SplitIndexFunction  ->  Split_index(str, delimiter, index)
 * description:
 * - Return NULL if the index is negative or any of the arguments is NULL
 * - Return NULL if the index is out of bounds of the split strings
 * - Return the string at the given 'index' integer(zero-based) after splitting 'str' by 'delimiter'
 */
@TransformFunction(names = {"split_index",
        "splitindex"}, parameter = "(String str, String delimiter, Integer index)", descriptions = {
                "- Return \"\" if the index is negative or any of the arguments is NULL;",
                "- Return \"\" NULL if the index is out of bounds of the split strings;",
                "- Return the string at the given 'index' integer(zero-based) after splitting 'str' by 'delimiter'."
        }, examples = {
                "split_index('a,b,c', ',', 1) = \"b\""
        })
public class SplitIndexFunction implements ValueParser {

    private final ValueParser strParser;
    private final ValueParser delimiterParser;
    private final ValueParser indexParser;

    public SplitIndexFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        strParser = OperatorTools.buildParser(expressions.get(0));
        delimiterParser = OperatorTools.buildParser(expressions.get(1));
        indexParser = OperatorTools.buildParser(expressions.get(2));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object strObject = strParser.parse(sourceData, rowIndex, context);
        Object delimiterObject = delimiterParser.parse(sourceData, rowIndex, context);
        Object indexObject = indexParser.parse(sourceData, rowIndex, context);

        if (strObject == null || delimiterObject == null || indexObject == null) {
            return null;
        }

        String str = OperatorTools.parseString(strObject);
        String delimiter = OperatorTools.parseString(delimiterObject);
        int index = OperatorTools.parseBigDecimal(indexObject).intValue();

        if (str == null || delimiter == null || index < 0) {
            return null;
        }

        String[] splitStrings = str.split(delimiter);
        if (index >= splitStrings.length) {
            return null;
        }

        return splitStrings[index];
    }
}