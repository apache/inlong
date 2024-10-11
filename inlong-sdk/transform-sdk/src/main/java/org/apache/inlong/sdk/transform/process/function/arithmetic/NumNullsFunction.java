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
import net.sf.jsqlparser.expression.NullValue;

import java.util.ArrayList;
import java.util.List;

/**
 * NumNullsFunction  ->  num_nulls(expr1,expr2,...)
 * description:
 * - Return the number of null arguments.
 */
@TransformFunction(names = {"num_nulls"}, parameter = "([Expr expr1, Expr expr2, ...])", descriptions = {
        "- Return the number of null arguments."
}, examples = {
        "num_nulls(5, null, null, null) = 3"
})
public class NumNullsFunction implements ValueParser {

    private final List<ValueParser> nodeList;

    public NumNullsFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        this.nodeList = new ArrayList<>();
        if (expressions == null || expressions.isEmpty()) {
            return;
        }
        for (Expression expression : expressions) {
            if (expression instanceof NullValue) {
                nodeList.add(null);
            } else {
                nodeList.add(OperatorTools.buildParser(expression));
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        int num = 0;
        for (ValueParser valueParser : nodeList) {
            if (valueParser == null) {
                num++;
                continue;
            }
            Object value = valueParser.parse(sourceData, rowIndex, context);
            if (value == null) {
                num++;
            }
        }
        return num;
    }
}
