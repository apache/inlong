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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ParseUrlFunction  ->  parse_url(urlStr, keyword[, parameter])
 * description:
 * - Return NULL any required parameter is NULL
 * - Return the specified part from the urlStr. Valid values for keyword.
 * Note: The keyword is one of ('HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'AUTHORITY', 'FILE', 'USERINFO').
 *       Also a value of a particular key in QUERY can be extracted by providing the key as the third argument.
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "parse_url"}, parameter = "(String urlStr, String keyword[, String parameter])", descriptions = {
                "- Return NULL any required parameter is NULL;",
                "- Return the specified part from the 'urlStr'. Valid values for 'keyword'.",
                "Note: The 'keyword' is one of ('HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'AUTHORITY', 'FILE', 'USERINFO'). "
                        +
                        "Also a value of a particular key in QUERY can be extracted by providing the key as the third argument."
        }, examples = {
                "parse_url('http://inlong.apache.org/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k1') = v1",
                "parse_url('http://inlong.apache.org/p.php?k1=v1&k2=v2#Ref1', 'PROTOCOL') = http"
        })
public class ParseUrlFunction implements ValueParser {

    private ValueParser urlParser;
    private ValueParser partParser;
    private ValueParser keyParser;

    public ParseUrlFunction(Function expr) {
        List<Expression> params = expr.getParameters().getExpressions();
        urlParser = OperatorTools.buildParser(params.get(0));
        partParser = params.size() > 1 ? OperatorTools.buildParser(params.get(1)) : null;
        keyParser = params.size() > 2 ? OperatorTools.buildParser(params.get(2)) : null;
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (urlParser == null || partParser == null) {
            return null;
        }
        Object urlObj = urlParser.parse(sourceData, rowIndex, context);
        Object partObj = partParser.parse(sourceData, rowIndex, context);
        Object keyObj = keyParser != null ? keyParser.parse(sourceData, rowIndex, context) : null;

        if (urlObj == null || partObj == null) {
            return null;
        }

        String url = OperatorTools.parseString(urlObj);
        String part = OperatorTools.parseString(partObj);
        String key = keyObj != null ? OperatorTools.parseString(keyObj) : null;
        if (keyParser != null && key == null) {
            return null;
        }

        try {
            URL netUrl = new URL(url);
            Map<String, String> queryPairs = splitQuery(netUrl.getQuery());
            if ("QUERY".equals(part)) {
                if (key != null && queryPairs.containsKey(key)) {
                    return queryPairs.get(key);
                }
                return netUrl.getQuery();
            } else {
                switch (part) {
                    case "HOST":
                        return netUrl.getHost();
                    case "PATH":
                        return netUrl.getPath();
                    case "REF":
                        return netUrl.getRef();
                    case "PROTOCOL":
                        return netUrl.getProtocol();
                    case "AUTHORITY":
                        return netUrl.getAuthority();
                    case "FILE":
                        return netUrl.getFile();
                    case "USERINFO":
                        return netUrl.getUserInfo();
                    default:
                        return null;
                }
            }
        } catch (MalformedURLException e) {
            return null;
        }
    }

    /**
     * splitQuery
     * @param query
     * @return java.util.Map<java.lang.String,java.lang.String>
     * @Description: Since Java 8 lacks URI decoding support available in Java 10+, URLDecoder.decode() cannot be used.
     * Therefore, a manual decoding method is implemented to extract query parameters.
    **/
    private Map<String, String> splitQuery(String query) {
        Map<String, String> queryPairs = new HashMap<>();
        if (query != null) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                int idx = pair.indexOf("=");
                if (idx != -1) { // skip if no "=" is found
                    queryPairs.put(manualDecode(pair.substring(0, idx)), manualDecode(pair.substring(idx + 1)));
                }
            }
        }
        return queryPairs;
    }

    private String manualDecode(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%') {
                if (i + 2 < s.length()) {
                    try {
                        int num = Integer.parseInt(s.substring(i + 1, i + 3), 16);
                        sb.append((char) num);
                        i += 2;
                    } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
                        // Invalid escape sequence or end of string reached
                        sb.append(c);
                    }
                } else {
                    // Invalid escape sequence
                    sb.append(c);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}