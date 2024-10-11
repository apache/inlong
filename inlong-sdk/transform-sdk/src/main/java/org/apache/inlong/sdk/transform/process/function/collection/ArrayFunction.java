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

package org.apache.inlong.sdk.transform.process.function.collection;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * ArrayFunction  ->  ARRAY(ANY1, ANY2, ...)
 * description:
 * - Return an array created from a list of values (value1, value2, …)
 */
@TransformFunction(names = {"array"}, parameter = "(String value1 [,String value2, ....])", descriptions = {
        "- Return an array created from a list of values ('value1', 'value2', ....)."
}, examples = {
        "array('he',7,'xxd') = [he, 7, xxd]",
        "array(array('he',5),'xxd') = return [[he, 5], xxd]",
        "array(array('he',5),array('','')) = return [[he, 5], [, ]]"
})
public class ArrayFunction implements ValueParser {

    private List<ValueParser> parserList;

    public ArrayFunction(Function expr) {
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
        ArrayList<Object> res = new ArrayList<>();
        for (ValueParser valueParser : parserList) {
            Object parseObj = valueParser.parse(sourceData, rowIndex, context);
            res.add(parseObj);
        }
        return res;
    }
}
