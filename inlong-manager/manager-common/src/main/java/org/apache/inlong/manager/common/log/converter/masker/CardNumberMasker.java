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
 * Masker that masks a potential card number (PAN) while still providing enough information to have it easily
 * identifiable for log analysis. The first digit as well as the last 6 digits won't be masked. All other digits
 * will be replaced by the mask character.
 *
 * A potential PAN is considered any sequence of digits that is between 8 and 19 characters long, surrounded by
 * whitespaces, and starts with known digits for PAN schemes (1, 3, 4, 5, or 6)
 *
 * Ex: 4916246076443617 -> 4*********443617
 */
public class CardNumberMasker implements LogMasker {
    private static final List<Character> KNOWN_PAN_START_DIGITS = Arrays.asList('1', '3', '4', '5', '6');

    private int startKeep = 1;
    private int endKeep = 6;

    @Override
    public void initialize(String args) {
        if (StringUtils.isBlank(args)) {
            startKeep = 1;
            endKeep = 6;
        } else {
            String[] params = StringUtils.split(args, '|');
            if (params.length != 2) {
                throw new ExceptionInInitializerError("Invalid parameters supplied for CardNumber masker: " + args);
            }
            startKeep = Integer.valueOf(params[0]);
            endKeep = Integer.valueOf(params[1]);

            if (startKeep < 1 || startKeep > 6) {
                throw new ExceptionInInitializerError("The number of unmasked digits at the start of the pan can't "
                        + "be more than 6 or less than 1");
            }

            if (endKeep < 1 || endKeep > 8) {
                throw new ExceptionInInitializerError("The number of unmasked digits at the end of the pan can't be "
                        + "more than 8 or less than 1");
            }
        }
    }

    @Override
    public int maskData(StringBuilder builder, char maskChar, int startPos, int buffLength) {
        int pos = startPos;
        int panLength;
        Character checkChar = builder.charAt(pos);
        if (KNOWN_PAN_START_DIGITS.contains(checkChar)) {
            panLength = 1;
            pos++;
            while (pos < buffLength && Character.isDigit(builder.charAt(pos))) {
                panLength++;
                pos++;
            }

            if (validPan(builder, startPos, panLength, buffLength)) {
                builder.replace(startPos + startKeep,
                        startPos + panLength - endKeep,
                        StringUtils.repeat(maskChar, panLength - startKeep - endKeep));

                return startPos + panLength;
            }
        }

        return startPos;
    }

    private boolean validPan(StringBuilder builder, int startPos, int panLength, int buffLength) {
        return panLength >= 8 && panLength <= 19
                && (startPos + panLength == buffLength || LogMasker.isDelimiter(builder.charAt(startPos + panLength)));
    }
}
