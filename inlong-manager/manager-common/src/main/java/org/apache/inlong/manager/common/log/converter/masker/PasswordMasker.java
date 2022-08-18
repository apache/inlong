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

package org.apache.inlong.manager.common.log.converter.masker;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * Masker that masks a potential password. The password must be precedent by one of the following keywords,
 * followed by ':' or '=' character. A space may also be present right before the password
 * Keywords:
 * <ul>
 *     <li>password</li>
 *     <li>pwd</li>
 *     <li>pass</li>
 * </ul>
 *
 * The password is replaced my 6 mask characters, regardless of the length of the actual password, so that no
 * information is provided about its strength.
 * The keyword is also replaced with 'password:'
 *
 * <strong>NOTE: </strong> This masker changes the length of the underlying string buffer
 *
 * Ex: password: testpass -> password: ******
 */
public class PasswordMasker implements LogMasker {
    private final List<String> keywords = Arrays.asList("password", "passphrase", "pwd", "pass", "secret_id",
            "token", "secret_key", "secret_token", "secretKey", "publicKey", "secretKey", "secretId");
    private final List<String> separators = Arrays.asList(":", "=", "\": \"", "\":\"", ">");

    @Override
    public int maskData(StringBuilder builder, char maskChar, int startPos, int buffLength) {
        int charPos = startPos;
        if (charPos + 5 > buffLength) {
            return startPos;
        }

        Character character = builder.charAt(charPos);
        if (isPasswordStart(character, builder, charPos)) {
            int keywordStart = 0;
            int keywordLength = 0;
            String keywordUsed = null;
            for (String keyword:keywords) {
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
            for (String separator:separators) {
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

    private int detectEnd(StringBuilder builder, int buffLength, int startPos, String keyword,
            int keywordLength, String separator) {
        if (separator.charAt(0) == '>') {
            return detectEndXml(builder, buffLength, startPos, keyword, keywordLength);
        } else if (separator.contains("\"")) {
            return detectEndJson(builder, buffLength, startPos);
        } else {
            return detectEndNoXml(builder, buffLength, startPos);
        }
    }

    private int detectEndNoXml(StringBuilder builder, int buffLength, int startPos) {
        while (startPos < buffLength && !LogMasker.isDelimiter(builder.charAt(startPos))) {
            startPos++;
        }

        return startPos;
    }

    private int detectEndJson(StringBuilder builder, int buffLength, int startPos) {
        while (startPos < buffLength && !isEndOfJson(builder, startPos)) {
            startPos++;
        }

        return startPos;
    }

    private boolean isEndOfJson(StringBuilder builder, int pos) {
        return builder.charAt(pos) == '"' && builder.charAt(pos - 1) != '\\';
    }

    private int detectEndXml(StringBuilder builder, int buffLength, int startPos, String keyword, int keywordLength) {
        if (buffLength < startPos + keywordLength + 3) {
            return -1;
        }

        int passwordEnd = StringUtils.indexOfIgnoreCase(builder, keyword, startPos);
        if (passwordEnd > 0 && builder.charAt(passwordEnd - 1) == '/' && builder.charAt(passwordEnd - 2) == '<') {
            return passwordEnd - 2;
        }

        return -1;
    }

    private boolean isPasswordStart(Character character, StringBuilder builder, int pos) {
        return 'p' == character || 'P' == character
                || ('<' == character && 'p' == builder.charAt(pos + 1))
                || ('<' == character && 'P' == builder.charAt(pos + 1))
                || 's' == character || 'S' == character
                || 't' == character || 'T' == character;
    }

    private boolean keywordStartAtRightPosition(int keywordStart, int pos) {
        return keywordStart >= 0 && (keywordStart == pos || keywordStart == pos + 1);
    }

    private int passwordStartPosition(int keywordStart, int keywordLength, String separator, StringBuilder builder) {
        int charPos = keywordStart + keywordLength + separator.length();
        if (Character.isWhitespace(builder.charAt(charPos))) {
            charPos++;
        }
        return charPos;
    }

    private int mask(StringBuilder builder, char maskChar, int startPos, int endPos) {
        final String masked = "" + maskChar + maskChar + maskChar + maskChar + maskChar + maskChar;
        builder.replace(startPos, endPos, masked);
        return startPos + 6;
    }
}
