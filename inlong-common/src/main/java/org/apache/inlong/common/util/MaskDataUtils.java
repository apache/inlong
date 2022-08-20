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

package org.apache.inlong.common.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * MaskDataUtils is used to mask sensitive information.
 */
public class MaskDataUtils {

    private static final List<String> KEYWORDS =
            Arrays.asList("password", "passphrase", "pwd", "pass", "secret_id", "token",
                    "secret_key", "secret_token", "secretKey", "publicKey", "secretKey",
                    "secretId");
    private static final List<String> SEPARATORS = Arrays.asList(":", "=", "\": \"", "\":\"");
    private static final List<Character> STOP_CHARACTERS = Arrays.asList('\'', '"');
    private static final List<Character> KNOWN_DELIMITERS =
            Collections.unmodifiableList(Arrays.asList('\'', '"', '<', '>'));

    public static void mask(StringBuilder stringBuilder) {
        boolean maskedThisCharacter;
        int pos;
        int newPos;
        int length = stringBuilder.length();
        for (pos = 0; pos < length; pos++) {
            maskedThisCharacter = false;
            newPos = maskData(stringBuilder, '*', pos, length);
            maskedThisCharacter = newPos != pos;
            if (maskedThisCharacter) {
                length = stringBuilder.length();
                maskedThisCharacter = false;
            }
            if (!maskedThisCharacter) {
                while (pos < length
                        && !(Character.isWhitespace(stringBuilder.charAt(pos))
                        || STOP_CHARACTERS.contains(stringBuilder.charAt(pos)))) {
                    pos++;
                }
            }
        }
    }

    private static int mask(StringBuilder builder, char maskChar, int startPos, int endPos) {
        final String masked = "" + maskChar + maskChar + maskChar + maskChar + maskChar + maskChar;
        builder.replace(startPos, endPos, masked);
        return startPos + 6;
    }

    public static int maskData(StringBuilder builder, char maskChar, int startPos, int buffLength) {
        int charPos = startPos;
        if (charPos + 5 > buffLength) {
            return startPos;
        }

        Character character = builder.charAt(charPos);
        if (isKeyWorkdStart(character)) {
            int keywordStart = 0;
            int keywordLength = 0;
            String keywordUsed = null;
            for (String keyword: KEYWORDS) {
                keywordStart = StringUtils.indexOfIgnoreCase(builder, keyword, charPos);
                if (keywordStartAtRightPosition(keywordStart, charPos)) {
                    keywordLength = keyword.length();
                    keywordUsed = keyword;
                    break;
                }
            }

            if (keywordStart != startPos && keywordStart != startPos + 1) {
                return startPos;
            }

            int idxSeparator;
            for (String separator: SEPARATORS) {
                idxSeparator = StringUtils.indexOf(builder, separator, keywordStart + keywordLength);
                if (idxSeparator == keywordStart + keywordLength) {
                    charPos = passwordStartPosition(keywordStart, keywordLength, separator, builder);

                    int endPos = detectEnd(builder, buffLength, charPos, keywordUsed, keywordLength, separator);

                    if (endPos > charPos) {
                        return mask(builder, maskChar, charPos, endPos);
                    }
                }
            }
        }

        return startPos;
    }

    private static int detectEnd(StringBuilder builder, int buffLength, int startPos, String keyword,
            int keywordLength, String separator) {
        if (separator.charAt(0) == '>') {
            return detectEndXml(builder, buffLength, startPos, keyword, keywordLength);
        } else if (separator.contains("\"")) {
            return detectEndJson(builder, buffLength, startPos);
        } else {
            return detectEndNoXml(builder, buffLength, startPos);
        }
    }

    private static int detectEndNoXml(StringBuilder builder, int buffLength, int startPos) {
        while (startPos < buffLength && !isDelimiter(builder.charAt(startPos))) {
            startPos++;
        }

        return startPos;
    }

    private static int detectEndJson(StringBuilder builder, int buffLength, int startPos) {
        while (startPos < buffLength && !isEndOfJson(builder, startPos)) {
            startPos++;
        }

        return startPos;
    }

    private static boolean isDelimiter(char character) {
        return Character.isWhitespace(character) || KNOWN_DELIMITERS.contains(character);
    }

    private static boolean isEndOfJson(StringBuilder builder, int pos) {
        return builder.charAt(pos) == '"' && builder.charAt(pos - 1) != '\\';
    }

    private static int detectEndXml(StringBuilder builder, int buffLength, int startPos,
            String keyword, int keywordLength) {
        if (buffLength < startPos + keywordLength + 3) {
            return -1;
        }

        int passwordEnd = StringUtils.indexOfIgnoreCase(builder, keyword, startPos);
        if (passwordEnd > 0 && builder.charAt(passwordEnd - 1) == '/' && builder.charAt(passwordEnd - 2) == '<') {
            return passwordEnd - 2;
        }

        return -1;
    }

    private static boolean isKeyWorkdStart(Character character) {
        boolean result = false;
        for (String keyword : KEYWORDS) {
            result = character.equals(keyword.charAt(0)) || result;
        }
        return result;
    }

    private static boolean keywordStartAtRightPosition(int keywordStart, int pos) {
        return keywordStart >= 0 && (keywordStart == pos || keywordStart == pos + 1);
    }

    private static int passwordStartPosition(int keywordStart, int keywordLength, String separator,
            StringBuilder builder) {
        int charPos = keywordStart + keywordLength + separator.length();
        if (Character.isWhitespace(builder.charAt(charPos))) {
            charPos++;
        }
        return charPos;
    }

}
