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

/**
 * FindInSetFunction  ->  FIND_IN_SET(str,strList)
 * Description:
 * - Return NULL if either argument is NULL
 * - Return 0 if str is not in strList or if strList is the empty string
 * - Return a value in the range of 1 to N if the string str is in the string list strList consisting of N substrings.
 * Note: strList is a string composed of substrings separated by ',' characters. This function does not work properly
 *       if the first argument contains a comma (,) character.
 */
@TransformFunction(type = FunctionConstant.COLLECTION_TYPE, names = {
        "find_in_set"}, parameter = "(String str,String strList)", descriptions = {
                "- Return \"\" if either argument is NULL;",
                "- Return 0 if 'str' is not in 'strList' or if 'strList' is the empty string;",
                "- Return a value in the range of 1 to N if the string 'str' is in the string list 'strList' consisting of N substrings.",
                "Note: strList is a string composed of substrings separated by ',' characters. This function does not work properly if the "
                        +
                        "first argument contains a comma (,) character."
        }, examples = {"FIND_IN_SET('b','a,b,b,c,d') = 2", "FIND_IN_SET('','a,,b,c,d') = 2"})
public class FindInSetFunction implements ValueParser {

    private final ValueParser strParser;
    private final ValueParser strListParser;

    public FindInSetFunction(Function expr) {
        strParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
        strListParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object strObj = strParser.parse(sourceData, rowIndex, context);
        Object strListObj = strListParser.parse(sourceData, rowIndex, context);
        if (strObj == null || strListObj == null) {
            return null;
        }
        String str = OperatorTools.parseString(strObj);
        String strList = OperatorTools.parseString(strListObj);
        if (!strList.isEmpty()) {
            String[] strArray = strList.split(",");
            for (int i = 0; i < strArray.length; i++) {
                if (str.equals(strArray[i])) {
                    return i + 1;
                }
            }
        }
        return 0;
    }
}
