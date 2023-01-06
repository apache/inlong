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

package org.apache.inlong.sort.base.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The pattern replace utils
 */
public final class PatternReplaceUtils {

    private static final Pattern REGEX_PATTERN = Pattern.compile("\\$\\{\\s*([\\w.-]+)\\s*}",
            Pattern.CASE_INSENSITIVE);

    public static String replace(String pattern, Map<String, String> params) {
        if (pattern == null) {
            return pattern;
        }
        Matcher matcher = REGEX_PATTERN.matcher(pattern);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String keyText = matcher.group(1);
            String replacement = params.get(keyText);
            if (replacement == null) {
                continue;
            }
            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * fills the paramMap with user defined ${} functions. Similar to jsonDynamicFormat.parse, but designed specifically for dirty data scenerios.
     * @param actualIdentifier a list of actual database/table/etc names to replace the ${} in identifier
     * @param paramMap the generated default paramMap
     * @param identifier the dirty identifier which has the literal regex wrapped in ${} to replace.
     */
    public static void fillParamMap(List<String> actualIdentifier, Map<String, String> paramMap, String identifier)
            throws IOException {
        Matcher matcher = REGEX_PATTERN.matcher(identifier);
        int i = 0;
        while (matcher.find()) {
            try {
                String keyText = matcher.group(1);
                String s = actualIdentifier.get(i);
                paramMap.put(keyText, s);
            } catch (Exception e) {
                throw new IOException("param map replacement failed");
            }
            i++;
        }
    }

}
