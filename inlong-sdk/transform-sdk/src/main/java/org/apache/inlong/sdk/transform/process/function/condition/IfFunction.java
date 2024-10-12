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
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.ExpressionOperator;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

/**
 * IfFunction  ->  if(expr,r1,r2)
 * description:
 * - Return r1 if expr holds
 * - Return r2 otherwise
 */
@TransformFunction(type = FunctionConstant.CONDITION_TYPE, names = {
        "if"}, parameter = "(Expr expr1,Expr expr2,Expr expr3)", descriptions = {
                "- Return 'expr2' if 'expr1' returns true;",
                "- Return 'expr3' otherwise."}, examples = {"if(1 = 1,true,false) = true"})
public class IfFunction implements ValueParser {

    private final ExpressionOperator expressionOperator;
    private final ValueParser tureValueParser;
    private final ValueParser falseValueParser;

    public IfFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        expressionOperator = OperatorTools.buildOperator(expressions.get(0));
        tureValueParser = OperatorTools.buildParser(expressions.get(1));
        falseValueParser = OperatorTools.buildParser(expressions.get(2));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        boolean condition = expressionOperator.check(sourceData, rowIndex, context);
        return condition ? tureValueParser.parse(sourceData, rowIndex, context)
                : falseValueParser.parse(sourceData, rowIndex, context);
    }
}
