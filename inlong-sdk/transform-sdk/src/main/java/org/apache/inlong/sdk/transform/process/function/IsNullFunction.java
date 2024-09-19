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
import org.apache.inlong.sdk.transform.process.operator.ExpressionOperator;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

/**
 * IsNullFunction
 * description: isnull(expr)
 * - return true if expr is NULL
 * - return false otherwise.
 */
@TransformFunction(names = {"isnull"})
public class IsNullFunction implements ValueParser {

    private ValueParser stringParser;
    private ExpressionOperator operator;

    public IsNullFunction(Function expr) {
        Expression expression = expr.getParameters().getExpressions().get(0);
        try {
            stringParser = OperatorTools.buildParser(expression);
        } catch (Exception e) {
            operator = OperatorTools.buildOperator(expression);
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object val = null;
        try {
            if (stringParser != null) {
                val = stringParser.parse(sourceData, rowIndex, context);
            } else {
                val = operator.check(sourceData, rowIndex, context);
            }
        } catch (Exception ignored) {

        }
        return val == null;
    }
}
