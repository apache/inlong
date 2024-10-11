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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * EltFunction -> elt(index, expr[, exprs]*)
 * description:
 * - Returns the index-th expression.
 * - index must be an integer between 1 and the number of expressions.
 * - Returns NULL if index is NULL or out of range.
 */
@TransformFunction(names = {"elt"})
public class EltFunction implements ValueParser {

    private ValueParser indexParser;
    private List<ValueParser> exprParsers;

    public EltFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        indexParser = OperatorTools.buildParser(expressions.get(0));
        exprParsers = new ArrayList<>();
        for (int i = 1; i < expressions.size(); i++) {
            exprParsers.add(OperatorTools.buildParser(expressions.get(i)));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object indexObject = indexParser.parse(sourceData, rowIndex, context);
        if (indexObject == null) {
            return null;
        }

        int index = OperatorTools.parseBigDecimal(indexObject).intValue();
        if (index < 1 || index > exprParsers.size()) {
            return null;
        }

        return exprParsers.get(index - 1).parse(sourceData, rowIndex, context);
    }
}