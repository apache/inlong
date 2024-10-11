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

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

/**
 * ContainsFunction  ->  contains(left, right)
 * description:
 * - Return NULL if left or right is NULL
 * - Return True if right is found inside left
 * - Return False otherwise
 */
@Slf4j
@TransformFunction(names = {"contains"}, parameter = "(String leftStr , String rightStr)", descriptions = {
        "- Return \"\" if 'leftStr' or rightStr is NULL;",
        "- Return True if 'rightStr' is found inside 'leftStr';",
        "- Return False otherwise."
}, examples = {
        "contains('Transform SQL', 'SQL') = true"
})
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
        if (leftStrObj == null || rightStrObj == null) {
            return null;
        }
        String leftStr = OperatorTools.parseString(leftStrObj);
        String rightStr = OperatorTools.parseString(rightStrObj);
        return (leftStr == null || rightStr == null) ? null : leftStr.contains(rightStr);
    }
}
