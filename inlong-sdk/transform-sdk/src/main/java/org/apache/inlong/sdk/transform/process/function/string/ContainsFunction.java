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
 * ContainsFunction
 * description: contains(left, right) - Returns a boolean.
 * The value is True if right is found inside left, otherwise, returns False.
 * Both left or right must be of STRING type.
 */
@TransformFunction(names = {"contains"})
public class ContainsFunction implements ValueParser {

    private ValueParser leftStrParser;
    private ValueParser rightStrParser;

    public ContainsFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        leftStrParser = OperatorTools.buildParser(expressions.get(0));
        rightStrParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object leftStrObj = leftStrParser.parse(sourceData, rowIndex, context);
        Object rightStrObj = rightStrParser.parse(sourceData, rowIndex, context);
        String leftStr = OperatorTools.parseString(leftStrObj);
        String rightStr = OperatorTools.parseString(rightStrObj);
        return (leftStr == null || rightStr == null) ? null : leftStr.contains(rightStr);
    }
}
