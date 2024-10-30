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
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * CoalesceFunction  ->  COALESCE(value1 [, value2, ...])
 * description:
 * - Return the first argument that is not NULL
 * - Return NULL If all arguments are NULL
 * Note: The return type is the least restrictive, common type of all of its arguments.
 * The return type is nullable if all arguments are nullable as well.
 */
@TransformFunction(type = FunctionConstant.CONDITION_TYPE, names = {
        "coalesce"}, parameter = "(String value1 [, String value2, ...])", descriptions = {
                "- Return \"\" If all arguments are NULL or \"\";",
                "- Return the first argument that is not NULL or \"\".",
                "Note: The return type is the least restrictive, common type of all of its arguments. The return type "
                        +
                        "is nullable if all arguments are nullable as well."
        }, examples = {"coalesce('', 'SQL', 'hh') = \"SQL\""})
public class CoalesceFunction implements ValueParser {

    private List<ValueParser> parserList;

    public CoalesceFunction(Function expr) {
        if (expr.getParameters() == null) {
            this.parserList = new ArrayList<>();
        } else {
            List<Expression> params = expr.getParameters().getExpressions();
            parserList = new ArrayList<>(params.size());
            for (Expression param : params) {
                ValueParser node = OperatorTools.buildParser(param);
                parserList.add(node);
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        for (ValueParser node : parserList) {
            Object parseObj = node.parse(sourceData, rowIndex, context);
            Object valueObj = parseValue(parseObj);
            if (valueObj != null) {
                return parseValue(parseObj);
            }
        }
        return null;
    }

    private Object parseValue(Object value) {
        Object parsedValue;
        if (value instanceof BigDecimal) {
            parsedValue = OperatorTools.parseBigDecimal(value);
        } else if (value instanceof Timestamp) {
            parsedValue = OperatorTools.parseTimestamp(value);
        } else if (value instanceof Date) {
            parsedValue = OperatorTools.parseDate(value);
        } else if (value instanceof byte[]) {
            parsedValue = OperatorTools.parseBytes(value);
        } else {
            parsedValue = OperatorTools.parseString(value);
        }
        // invalid
        if (parsedValue instanceof String && ((String) parsedValue).isEmpty()) {
            return null;
        }

        return parsedValue;
    }
}
