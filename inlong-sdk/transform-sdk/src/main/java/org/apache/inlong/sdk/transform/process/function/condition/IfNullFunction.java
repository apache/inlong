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

package org.apache.inlong.sdk.transform.process.function.condition;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

/**
 * IfNullFunction
 * description: IFNULL(expr1,expr2)
 * - return expr1 if expr1 is not NULL
 * - return expr2 otherwise
 */
@Slf4j
@TransformFunction(names = {"ifnull", "if_null"})
public class IfNullFunction implements ValueParser {

    private final ValueParser firstExprParser;
    private final ValueParser secondExprParser;

    public IfNullFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        firstExprParser = OperatorTools.buildParser(expressions.get(0));
        secondExprParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        try {
            Object firstExprObj = firstExprParser.parse(sourceData, rowIndex, context);
            if (firstExprObj == null) {
                return parseSecondExpr(sourceData, rowIndex, context);
            }
            return firstExprObj;
        } catch (Exception e) {
            log.error("Value parsing failed", e);
            return parseSecondExpr(sourceData, rowIndex, context);
        }
    }

    private Object parseSecondExpr(SourceData sourceData, int rowIndex, Context context) {
        try {
            return secondExprParser.parse(sourceData, rowIndex, context);
        } catch (Exception e) {
            log.error("Value parsing failed", e);
            return null;
        }
    }
}
