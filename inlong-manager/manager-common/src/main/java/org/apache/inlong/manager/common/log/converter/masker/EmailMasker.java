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

import org.apache.commons.lang3.StringUtils;

/**
 * Masker that masks a potential email address while still providing enough information to have it easily identifiable
 * for log analysis. The first and last character of the address as well as the domain name are left intact while the
 * rest are replaced with the mask character.
 *
 * Ex: test.email@domain.com -> t********l@d****n.com
 */
public class EmailMasker implements LogMasker {
    @Override
    public int maskData(StringBuilder builder, char maskChar, int startPos, int buffLength) {
        if (startPos == 0) {
            return startPos;
        }

        int pos = startPos;
        int indexOfAt;
        int indexOfDot;
        int indexOfEnd;
        int emailStartPos;
        char character = builder.charAt(pos - 1);
        if ('@' == character) {
            indexOfAt = pos - 1;
            pos--;
            emailStartPos = indexOfStart(builder, pos, indexOfAt);
            indexOfEnd = indexOfEmailEnd(builder, indexOfAt + 1, emailStartPos, buffLength);
            indexOfDot = indexOfDot(indexOfAt, indexOfEnd, builder);

            if (emailStartPos < indexOfAt && indexOfAt < indexOfDot && indexOfDot < indexOfEnd) {
                builder.replace(emailStartPos + 1, indexOfAt - 1,
                                StringUtils.repeat(maskChar, indexOfAt - emailStartPos - 2))
                        .replace(indexOfAt + 2, indexOfDot - 1,
                                StringUtils.repeat(maskChar, indexOfDot - indexOfAt - 3));
                return indexOfEnd;
            }
        }

        return startPos;
    }

    private int indexOfStart(StringBuilder unmasked, int pos, int indexOfAt) {
        while (pos >= 0 && !(LogMasker.isDelimiter(unmasked.charAt(pos)))) {
            pos--;
            if (pos > 0 && unmasked.charAt(pos) == '@') {
                return indexOfAt;
            }
        }
        return pos + 1;
    }

    private int indexOfEmailEnd(StringBuilder unmasked, int startPost, int emailStart, int buffLength) {
        while (startPost < buffLength) {
            if (LogMasker.isDelimiter(unmasked.charAt(startPost))) {
                return startPost - 1;
            } else if ('@' == unmasked.charAt(startPost)) {
                // we discovered another '@' character, so we set the end at the start, since it is NOT a proper email
                return emailStart;
            }
            startPost++;
        }
        return startPost - 1;
    }

    private int indexOfDot(int startPos, int endPos, StringBuilder unmasked) {
        while (endPos > startPos && unmasked.charAt(endPos) != '.') {
            endPos--;
        }

        return endPos;
    }
}
