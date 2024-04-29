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

package org.apache.inlong.sdk.transform.encode;

import java.util.List;

/**
 * EscapeUtils
 * 
 */
public class EscapeUtils {

    public static String escapeStringSeparator(String fieldValue, char separator) {
        StringBuilder builder = new StringBuilder();
        escapeContent(builder, separator, '\\', fieldValue);
        String formatField = builder.toString();
        return formatField;
    }

    public static String escapeFields(List<String> fields, char separator) {
        if (fields.size() <= 0) {
            return "";
        }
        StringBuilder ss = new StringBuilder();
        for (String field : fields) {
            String fmtField = escapeStringSeparator(field, separator);
            ss.append(fmtField).append(separator);
        }
        String result = ss.substring(0, ss.length() - 1);
        return result;
    }

    public static void escapeContent(StringBuilder builder, char separator, char escapeChar, Object field) {
        String strField = "";
        if (field != null) {
            strField = String.valueOf(field);
        }
        int length = strField.length();

        for (int i = 0; i < length; i++) {
            putValueIntoStringBuilder(builder, separator, escapeChar, strField.charAt(i));
        }
    }

    public static void putValueIntoStringBuilder(StringBuilder builder, char separator, final char escapeChar,
            char value) {
        switch (value) {
            case 0:
                builder.append(escapeChar).append('0');
                break;
            case '\n':
                builder.append(escapeChar).append('n');
                break;
            case '\r':
                builder.append(escapeChar).append('r');
                break;
            default:
                if (value == separator) {
                    builder.append(escapeChar).append(separator);
                } else if (value == escapeChar) {
                    builder.append(escapeChar).append(escapeChar);
                } else {
                    builder.append(value);
                }
                break;
        }
    }
}
