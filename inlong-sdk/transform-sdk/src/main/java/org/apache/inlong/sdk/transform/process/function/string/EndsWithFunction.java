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
 * EndsWithFunction
 * description: Returns whether expr ends with endExpr.
 */
@TransformFunction(names = {"endswith"})
public class EndsWithFunction implements ValueParser {

    private ValueParser exprParser;
    private ValueParser endExprParser;

    public EndsWithFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        if (expressions != null && expressions.size() == 2) {
            exprParser = OperatorTools.buildParser(expressions.get(0));
            endExprParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object exprObj = exprParser.parse(sourceData, rowIndex, context);
        Object endExprObj = endExprParser.parse(sourceData, rowIndex, context);

        if (exprObj == null || endExprObj == null) {
            return null;
        }

        if (!isSameType(exprObj, endExprObj)) {
            throw new IllegalArgumentException("Both arguments must be of the same type.");
        }

        if (exprObj instanceof byte[] && endExprObj instanceof byte[]) {
            String exprString = new String((byte[]) exprObj);
            String endExprString = new String((byte[]) endExprObj);
            if (endExprString.isEmpty()) {
                return true;
            }

            return exprString.endsWith(endExprString);
        }

        String exprString = OperatorTools.parseString(exprObj);
        String endExprString = OperatorTools.parseString(endExprObj);

        if (endExprString.isEmpty()) {
            return true;
        }

        return exprString.endsWith(endExprString);
    }

    private boolean isSameType(Object a, Object b) {
        return (a instanceof String && b instanceof String) || (a instanceof byte[] && b instanceof byte[]);
    }
}
