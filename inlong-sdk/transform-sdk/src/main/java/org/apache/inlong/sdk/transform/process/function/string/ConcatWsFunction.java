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

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NullValue;

import java.util.ArrayList;
import java.util.List;

/**
 * ConcatWsFunction  ->  concat_ws(string1, string2, string3,...)
 * description:
 * - Return NULL If STRING1 is NULL.
 * - Return a string that concatenates (STRING2, STRING3, ...) with a separator STRING1.
 */
@Slf4j
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "concat_ws"}, parameter = "(String string1 [, String string2, ...])", descriptions = {
                "- Return NULL If 'STRING1' is NULL;",
                "- Return a string that concatenates ('STRING2', 'STRING3', ...) with a separator STRING1."
        }, examples = {
                "concat_ws('-', 'apple', 'banana', 'cloud') = \"apple-banana-cloud\"",
                "concat_ws('-', 'apple', '', 'cloud') = \"apple--cloud\"",
                "concat_ws('-', 'apple', null, 'cloud') = \"apple-cloud\""
        })
public class ConcatWsFunction implements ValueParser {

    private final String separator;
    private final List<ValueParser> nodeList;

    public ConcatWsFunction(Function expr) {
        List<Expression> params = expr.getParameters().getExpressions();
        if (params == null || params.isEmpty()) {
            this.separator = null;
            this.nodeList = new ArrayList<>();
            return;
        }

        // Handle the case where the separator is NULL
        Expression separatorExpr = params.get(0);
        if (separatorExpr instanceof NullValue) {
            this.separator = null;
        } else {
            this.separator = OperatorTools.parseString(
                    OperatorTools.buildParser(separatorExpr).parse(null, 0, null));
        }

        this.nodeList = new ArrayList<>(params.size() - 1);
        for (int i = 1; i < params.size(); i++) {
            Expression paramExpr = params.get(i);
            if (paramExpr instanceof NullValue) {
                // Add null to the list to be handled in parse method
                nodeList.add(null);
            } else {
                nodeList.add(OperatorTools.buildParser(paramExpr));
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (separator == null) {
            return null;
        }
        StringBuilder result = new StringBuilder();
        boolean firstStrFlag = true;
        for (ValueParser node : nodeList) {
            if (node != null) {
                Object parsedValue = node.parse(sourceData, rowIndex, context);
                if (parsedValue != null) {
                    if (!firstStrFlag) {
                        result.append(separator);
                    }
                    result.append(parsedValue);
                    firstStrFlag = false;
                }
            }
        }
        return result.toString();
    }
}
