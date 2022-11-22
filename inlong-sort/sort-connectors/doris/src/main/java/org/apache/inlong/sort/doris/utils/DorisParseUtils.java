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

package org.apache.inlong.sort.doris.utils;

import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.inlong.common.metric.MetricObserver.LOG;

/**
 * A utility class primarily serving DorisDynamaicSchemaOutputFormat
 * extracts some common utility methods out.
 */
public class DorisParseUtils {

    private static final String ESCAPE = "\\\\x(\\d{2})";

    /**
     * A utility function used to split the given string which represents a captured row,
     * containing whitespace or tab and parse it to a hashmap.
     *
     * @param data an object which is created by stringify row data
     * @return Map a hashmap which can be used to load the data
     */
    public static Map<String, String> parsetoMap(Object data) {
        String[] toParse = data.toString().split("\\s+");
        Map<String, String> ret = new HashMap<>();
        if (toParse.length < 2) {
            LOG.warn("parse length insufficient! string is :{}", Arrays.toString(toParse));
            return ret;
        }
        ret.put("id", toParse[0]);
        ret.put("__DORIS_DELETE_SIGN__", toParse[1]);
        return ret;
    }

    /**
     * A utility function used to determine the DORIS_DELETE_SIGN for a row change.
     *
     * @param rowKind the row change
     * @return the doris delete sign corresponding to the change
     */
    public static String parseDeleteSign(RowKind rowKind) {
        if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
            return "0";
        } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
            return "1";
        } else {
            throw new RuntimeException("Unrecognized row kind:" + rowKind.toString());
        }
    }

    /**
     * A utility function used to parse a string according to the given hexadecimal escape sequence.
     * example input: ""hi\\x33hi\\x44hello"" , where \x33 is '!', \x44 is ','
     * example output: "hi!hi,hello"
     *
     * @param s string before parsing
     * @return the parsed string
     */
    public static String escapeString(String s) {
        Pattern p = Pattern.compile(ESCAPE);
        Matcher m = p.matcher(s);

        StringBuffer buf = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(buf, String.format("%s", (char) Integer.parseInt(m.group(1))));
        }
        m.appendTail(buf);

        return buf.toString();
    }

}
