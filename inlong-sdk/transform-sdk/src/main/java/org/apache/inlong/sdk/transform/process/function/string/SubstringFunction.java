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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import java.util.List;

/**
 * SubstringFunction -> substring(string FROM INT1 [ FOR INT2 ])
 * description:
 * return a substring of STRING starting from position INT1 with length INT2 (to the end by default)
 */
@TransformFunction(names = {"substring", "substr", "mid"})
public class SubstringFunction implements ValueParser {

    private ValueParser stringParser;
    private ValueParser startPositionParser;
    private ValueParser lengthParser;

    public SubstringFunction(Function expr) {
        ExpressionList parameters = expr.getParameters();
        List<Expression> expressions;
        if (parameters != null) {
            expressions = parameters.getExpressions();
        } else {
            expressions = expr.getNamedParameters().getExpressions();
        }
        stringParser = OperatorTools.buildParser(expressions.get(0));
        startPositionParser = OperatorTools.buildParser(expressions.get(1));
        if (expressions.size() == 3) {
            lengthParser = OperatorTools.buildParser(expressions.get(2));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObj = stringParser.parse(sourceData, rowIndex, context);
        Object startPositionObj = startPositionParser.parse(sourceData, rowIndex, context);
        String str = OperatorTools.parseString(stringObj);
        int start = OperatorTools.parseBigDecimal(startPositionObj).intValue();
        if (start > str.length()) {
            return "";
        }
        if (lengthParser != null) {
            Object lengthObj = lengthParser.parse(sourceData, rowIndex, context);
            int len = OperatorTools.parseBigDecimal(lengthObj).intValue();
            if (len <= 0) {
                return "";
            }
            return str.substring(Math.max(start - 1, 0), Math.min(start - 1 + len, str.length()));
        } else {
            return str.substring(Math.max(start - 1, 0));
        }
    }
}
