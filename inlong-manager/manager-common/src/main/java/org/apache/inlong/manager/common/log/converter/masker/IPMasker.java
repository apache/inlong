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

/**
 * Masker that masks a potential IBAN while still providing enough information to have it easily identifiable for
 * log analysis. Only the first character of each group is retained, while the rest is replaced with the mask character.
 *
 * Ex: 84.232.150.27 -> 8*.2**.1**.2*
 */
public class IPMasker implements LogMasker {
    @Override
    public int maskData(StringBuilder builder, char maskChar, int startPos, int buffLength) {
        int pos = startPos;
        Character character = builder.charAt(pos);
        if (Character.isDigit(character)) {
            int noDigits = 1;
            int noDots = 0;
            pos++;
            while (pos < buffLength && !LogMasker.isDelimiter(builder.charAt(pos))) {
                character = builder.charAt(pos);
                pos++;
                if (Character.isDigit(character)) {
                    noDigits++;
                    if (noDigits > 3) {
                        return startPos;
                    }
                } else if ('.' == character) {
                    noDots++;
                    noDigits = 0;
                } else {
                    return startPos;
                }
            }

            if (noDots == 3 || isDotAtEnd(noDots, builder, pos, buffLength)) {
                StringBuilder masked = new StringBuilder();
                int consecutiveDigits = 0;
                for (int charPos = startPos; charPos < pos; charPos++) {
                    if ('.' == builder.charAt(charPos)) {
                        masked.append('.');
                        consecutiveDigits = 0;
                    } else if (consecutiveDigits == 0) {
                        masked.append(builder.charAt(charPos));
                        consecutiveDigits++;
                    } else {
                        masked.append(maskChar);
                    }
                }

                builder.replace(startPos, pos, masked.toString());
                return pos;
            }
        }

        return startPos;
    }

    private boolean isDotAtEnd(int noDots, StringBuilder builder, int charPos, int buffLength) {
        return noDots == 4
                && (charPos == buffLength || LogMasker.isDelimiter(builder.charAt(charPos)));
    }
}
