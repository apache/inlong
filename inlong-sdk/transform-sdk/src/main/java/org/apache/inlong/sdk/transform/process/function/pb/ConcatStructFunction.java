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

package org.apache.inlong.sdk.transform.process.function.pb;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.flink.table.data.GenericRowData;

import java.util.ArrayList;
import java.util.List;

/**
 * ConcatStructFunction  ->  concat_struct(field1, field2, field3...)
 * description:
 * - Always returns a GenericRowData whose arity equals the number of input parameters.
 * - If any parameter evaluates to NULL, the corresponding position in the returned
 *   GenericRowData is set to NULL while the other positions are populated normally.
 * - Each field value is taken from the protobuf source data based on its path.
 */
@TransformFunction(type = FunctionConstant.PB_TYPE, names = {
        "concat_struct"}, parameter = "(field1,field2,field3...)", descriptions = {
                "- Always returns a GenericRowData whose arity equals the number of input parameters;",
                "- If any parameter is NULL, the corresponding position in the returned "
                        + "GenericRowData is set to NULL while the other positions are populated normally;",
                "- Each field value is taken from the protobuf source data based on its 'path'."
        }, examples = {
                "concat_struct($root.name,$root.age) = +I(\"Alice\",11)"
        })
public class ConcatStructFunction implements ValueParser {

    private final List<ValueParser> fieldParsers;

    public ConcatStructFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        this.fieldParsers = new ArrayList<>();
        for (int i = 0; i < expressions.size(); i++) {
            this.fieldParsers.add(OperatorTools.buildParser(expressions.get(i)));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        GenericRowData result = new GenericRowData(fieldParsers.size());
        int index = 0;
        for (ValueParser parser : fieldParsers) {
            result.setField(index++, parser.parse(sourceData, rowIndex, context));
        }
        return result;
    }
}
