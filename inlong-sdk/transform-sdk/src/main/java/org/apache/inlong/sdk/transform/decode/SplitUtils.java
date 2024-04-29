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

package org.apache.inlong.sdk.transform.decode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * SplitUtils
 * 
 */
public class SplitUtils {

    public static final int STATE_NORMAL = 0;
    public static final int STATE_KEY = 2;
    public static final int STATE_VALUE = 4;
    public static final int STATE_ESCAPING = 8;
    public static final int STATE_QUOTING = 16;

    public static String[] splitCsv(@Nonnull String text, @Nonnull Character delimiter, @Nullable Character escapeChar,
            @Nullable Character quoteChar, boolean hasEscapeProcess) {
        String[][] splitResult = splitCsv(text, delimiter, escapeChar, quoteChar, null, hasEscapeProcess);
        if (splitResult.length == 0) {
            return new String[0];
        }
        return splitResult[0];
    }

    public static String[][] splitCsv(@Nonnull String text, @Nonnull Character delimiter,
            @Nullable Character escapeChar, @Nullable Character quoteChar, @Nullable Character lineDelimiter,
            boolean hasEscapeProcess) {
        return splitCsv(text, delimiter, escapeChar, quoteChar, lineDelimiter, false, hasEscapeProcess);
    }

    public static String[][] splitCsv(@Nonnull String text, @Nonnull Character delimiter,
            @Nullable Character escapeChar, @Nullable Character quoteChar, @Nullable Character lineDelimiter,
            boolean deleteHeadDelimiter, boolean hasEscapeProcess) {
        char deli = delimiter.charValue();
        char escape = (escapeChar == null) ? '\\' : escapeChar.charValue();
        char quote = (quoteChar == null) ? '\"' : quoteChar.charValue();
        char line = (lineDelimiter == null) ? '\n' : lineDelimiter.charValue();
        List<String[]> lines = new ArrayList<>();
        List<String> fields = new ArrayList<>();

        int state = STATE_NORMAL;

        char[] srcValue = text.toCharArray();
        char[] fieldValue = new char[srcValue.length];
        int fieldIndex = 0;
        for (int i = 0; i < text.length(); i++) {
            char ch = srcValue[i];

            if (ch == deli) {
                switch (state) {
                    case STATE_NORMAL:
                        if (fieldIndex == 0 && deleteHeadDelimiter && fields.isEmpty()) {
                            break;
                        }
                        fields.add(new String(fieldValue, 0, fieldIndex));
                        fieldIndex = 0;
                        break;
                    case STATE_ESCAPING:
                        fieldValue[fieldIndex++] = ch;
                        state = STATE_NORMAL;
                        break;
                    case STATE_QUOTING:
                        fieldValue[fieldIndex++] = ch;
                        break;
                    default:
                        break;
                }
            } else if (escapeChar != null && ch == escape) {
                switch (state) {
                    case STATE_NORMAL:
                        state = STATE_ESCAPING;
                        break;
                    case STATE_ESCAPING:
                        if (!hasEscapeProcess) {
                            fieldValue[fieldIndex++] = escapeChar;
                        }
                        fieldValue[fieldIndex++] = ch;
                        state = STATE_NORMAL;
                        break;
                    case STATE_QUOTING:
                        fieldValue[fieldIndex++] = ch;
                        break;
                    default:
                        break;
                }
            } else if (quoteChar != null && ch == quote) {
                switch (state) {
                    case STATE_NORMAL:
                        state = STATE_QUOTING;
                        break;
                    case STATE_ESCAPING:
                        if (!hasEscapeProcess) {
                            fieldValue[fieldIndex++] = escapeChar;
                        }
                        fieldValue[fieldIndex++] = ch;
                        state = STATE_NORMAL;
                        break;
                    case STATE_QUOTING:
                        state = STATE_NORMAL;
                        break;
                    default:
                        break;
                }
            } else if (lineDelimiter != null && ch == line) {
                switch (state) {
                    case STATE_NORMAL:
                        fields.add(new String(fieldValue, 0, fieldIndex));
                        fieldIndex = 0;
                        lines.add(fields.toArray(new String[0]));
                        fields.clear();
                        break;
                    case STATE_ESCAPING:
                        fieldValue[fieldIndex++] = ch;
                        state = STATE_NORMAL;
                        break;
                    case STATE_QUOTING:
                        fieldValue[fieldIndex++] = ch;
                        break;
                    default:
                        break;
                }
            } else {
                if (state == STATE_ESCAPING) {
                    if (!hasEscapeProcess) {
                        fieldValue[fieldIndex++] = escapeChar;
                    }
                    state = STATE_NORMAL;
                }
                fieldValue[fieldIndex++] = ch;
            }
        }
        fields.add(new String(fieldValue, 0, fieldIndex));
        fieldIndex = 0;
        lines.add(fields.toArray(new String[0]));

        String[][] result = new String[lines.size()][];
        for (int i = 0; i < lines.size(); i++) {
            result[i] = lines.get(i);
        }
        return result;
    }
}
