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
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
/**
 * StrToMapFunction  ->  str_to_map(str, pairDelimiter, keyValueDelimiter)
 * description:
 * - Return NULL if 'str' is NULL
 * - Return a map after splitting the 'str' into key/value pairs using 'pairDelimiter'(default is ',')
 *          and 'keyValueDelimiter'(default is '=')
 * Note: Both 'pairDelimiter' and 'keyValueDelimiter' are treated as regular expressions.So special characters
 *       (e.g. <([{^-=$!|]})?*+.>) need to be properly escaped before using as a delimiter literally.
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "str_to_map"}, parameter = "(String s1, String pairDelimiter, String keyValueDelimiter)", descriptions = {
                "- Return \"\" if 'str' is NULL;",
                "- Return a map after splitting the 'str' into key/value pairs using 'pairDelimiter'(default is ',') and 'keyValueDelimiter'(default is '=');",
                "Note: Both 'pairDelimiter' and 'keyValueDelimiter' are treated as regular expressions.So special characters(e.g. <([{^-=$!|]})?*+.>) need to be properly escaped before using as a delimiter literally."}, examples = {
                        "str_to_map('key1=value1,key2=value2,key3=value3') = {key1=value1, key2=value2, key3=value3}",
                        "str_to_map(\"name->John!age->30!city->China\" , \"!\" , \"->\") = {name=John, age=30, city=China}"})
public class StrToMapFunction implements ValueParser {

    private ValueParser inputParser;

    private ValueParser pairDelimiterParser;

    private ValueParser kvDelimiterParser;

    public StrToMapFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        if (!expressions.isEmpty()) {
            inputParser = OperatorTools.buildParser(expressions.get(0));
            if (expressions.size() >= 2) {
                pairDelimiterParser = OperatorTools.buildParser(expressions.get(1));
                if (expressions.size() >= 3) {
                    kvDelimiterParser = OperatorTools.buildParser(expressions.get(2));
                }
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object inputStringObj = inputParser.parse(sourceData, rowIndex, context);
        Object pairDelimiterStringObj = null;
        String pairDelimiterString = null;
        if (pairDelimiterParser != null) {
            pairDelimiterStringObj = pairDelimiterParser.parse(sourceData, rowIndex, context);
            pairDelimiterString = OperatorTools.parseString(pairDelimiterStringObj);
        }
        Object kvDelimiterStringObj = null;
        String kvDelimiterString = null;
        if (kvDelimiterParser != null) {
            kvDelimiterStringObj = kvDelimiterParser.parse(sourceData, rowIndex, context);
            kvDelimiterString = OperatorTools.parseString(kvDelimiterStringObj);
        }
        String inputString = OperatorTools.parseString(inputStringObj);

        return getStringStringMap(pairDelimiterString, kvDelimiterString, inputString);
    }

    private Map<String, String> getStringStringMap(String pairDelimiterString, String kvDelimiterString,
            String inputString) {
        String pairDelimiter =
                (pairDelimiterString == null || pairDelimiterString.isEmpty()) ? "," : escapeRegex(pairDelimiterString);
        String keyValueDelimiter =
                (kvDelimiterString == null || kvDelimiterString.isEmpty()) ? "=" : escapeRegex(kvDelimiterString);

        Map<String, String> map = new LinkedHashMap<>();
        String[] pairs = inputString.split(pairDelimiter);

        for (String pair : pairs) {
            if (pair.contains(keyValueDelimiter)) {
                String[] keyValue = pair.split(keyValueDelimiter, 2);
                map.put(keyValue[0], keyValue[1]);
            }
        }
        return map;
    }

    private String escapeRegex(String delimiter) {
        return delimiter.replaceAll("([\\\\^$|?*+\\[\\](){}])", "\\\\$1");
    }
}
