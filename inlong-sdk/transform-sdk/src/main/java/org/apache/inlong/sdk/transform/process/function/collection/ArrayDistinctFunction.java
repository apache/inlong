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

import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
/**
 * ArrayDistinctFunction
 * description: ARRAY_DISTINCT(haystack)--Returns an array with unique elements. If the array itself is null,
 *              the function will return null. Keeps ordering of elements.
 * for example: array_distinct(array('he',-1,'he'))--return [he, -1]
 */
@TransformFunction(names = {"array_distinct"})
public class ArrayDistinctFunction implements ValueParser {

    private final ValueParser arrayParser;

    public ArrayDistinctFunction(Function expr) {
        this.arrayParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object arrayObj = arrayParser.parse(sourceData, rowIndex, context);
        if (arrayObj == null) {
            return null;
        }
        if (arrayObj instanceof ArrayList) {
            ArrayList<?> array = (ArrayList<?>) arrayObj;
            if (array.isEmpty()) {
                return null;
            }
            Set<Object> distinctSet = new LinkedHashSet<>(array);
            return new ArrayList<>(distinctSet);
        }
        return null;
    }
}
