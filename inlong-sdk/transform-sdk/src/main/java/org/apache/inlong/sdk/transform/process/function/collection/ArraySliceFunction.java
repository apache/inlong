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
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
/**
 * ArraySliceFunction  ->  ARRAY_SLICE(array, start_offset[, end_offset])
 * description:
 * - Return NULL if 'array' or 'start_offset' is null;
 * - Return a subarray of the input 'array' between 'start_offset' and 'end_offset' inclusive;
 * - Return an empty array if 'start_offset' is after 'end_offset' or both are out of 'array' bounds.
 * Note: If 'end_offset' is omitted then this offset is treated as the length of the 'array'.
 *       Positive values are counted from the beginning of the array while negative from the end.
 */
@TransformFunction(type = FunctionConstant.COLLECTION_TYPE, names = {
        "array_slice"}, parameter = "(Array array, Integer start_offset[, Integer end_offset])", descriptions = {
                "- Return \"\" if 'array' or 'start_offset' is null;",
                "- Return a subarray of the input 'array' between 'start_offset' and 'end_offset' inclusive;",
                "- Return an empty array if 'start_offset' is after 'end_offset' or both are out of 'array' bounds.",
                "Note: If 'end_offset' is omitted then this offset is treated as the length of the 'array'. Positive values "
                        +
                        "are counted from the beginning of the array while negative from the end."
        }, examples = {
                "array_slice(array('he',7,'xxd'),1,2) = ['he', 7]",
                "array_slice(array('he','xxd','b'),-2,-1) = [3, 'xxd']"
        })
public class ArraySliceFunction implements ValueParser {

    private ValueParser arrayParser;

    private ValueParser startOffsetParser;

    private ValueParser endOffsetParser;

    public ArraySliceFunction(Function expr) {
        if (expr.getParameters().getExpressions().size() >= 3) {
            this.arrayParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));;
            this.startOffsetParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(1));;
            this.endOffsetParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(2));;
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object arrayObj = arrayParser.parse(sourceData, rowIndex, context);
        Object startOffsetObj = startOffsetParser.parse(sourceData, rowIndex, context);
        Object endOffsetObj = endOffsetParser.parse(sourceData, rowIndex, context);
        if (arrayObj == null || startOffsetObj == null || endOffsetObj == null) {
            return null;
        }
        int startOffset = OperatorTools.parseBigDecimal(startOffsetObj).intValue();
        int endOffset = OperatorTools.parseBigDecimal(endOffsetObj).intValue();
        if (arrayObj instanceof ArrayList) {
            ArrayList<?> array = (ArrayList<?>) arrayObj;
            if (array.isEmpty()) {
                return null;
            }
            int arrayLength = array.size();

            int startIndex = startOffset > 0 ? startOffset - 1 : startOffset == 0 ? 0 : arrayLength + startOffset;
            int endIndex = endOffset > 0 ? endOffset - 1 : endOffset == 0 ? 0 : arrayLength + endOffset;

            if (startIndex < 0 || endIndex < 0 || endIndex >= arrayLength || startIndex > endIndex) {
                return new ArrayList<>();
            }

            return new ArrayList<>(array.subList(startIndex, endIndex + 1));
        }
        return null;
    }
}
