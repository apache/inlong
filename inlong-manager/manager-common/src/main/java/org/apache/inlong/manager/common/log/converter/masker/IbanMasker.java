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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Masker that masks a potential IBAN while still providing enough information to have it easily identifiable for
 * log analysis. The first three and last 4 characters are preserved, while the others are replaced with the mask
 * character.
 *
 * A potential IBAN is considered any sequence of characters that starts with the country code, has the third
 * character a digit and has the right length. For performance/speed considerations, additional checks are
 * <strong>NOT</strong> made.
 *
 * Ex: GT20AGRO00000000001234567890 -> GT2*********************7890
 */
public class IbanMasker implements LogMasker {
    private Map<Character, Map<Character, Integer>> countriesToCheck = new HashMap<>(COUNTRIES_DICTIONARY);

    @Override
    public void initialize(String params) {
        if (StringUtils.isBlank(params)) {
            this.countriesToCheck = new HashMap<>(COUNTRIES_DICTIONARY);
            return;
        }

        String[] countryCodes = StringUtils.split(params.toUpperCase(Locale.ROOT), '|');
        this.countriesToCheck = new HashMap<>();
        for (String cc : countryCodes) {
            Map<Character, Integer> countries = COUNTRIES_DICTIONARY.get(cc.charAt(0));
            if (countries != null) {
                Map<Character, Integer> alreadyInDictionary = this.countriesToCheck.get(cc.charAt(0));
                Integer ibanLength = countries.get(cc.charAt(1));
                if (alreadyInDictionary == null && ibanLength != null) {
                    alreadyInDictionary = new HashMap<>();
                    this.countriesToCheck.put(cc.charAt(0), alreadyInDictionary);
                }
                if (ibanLength == null) {
                    throw new ExceptionInInitializerError("Invalid country provided: " + cc);
                }
                alreadyInDictionary.put(cc.charAt(1), ibanLength);
            } else {
                throw new ExceptionInInitializerError("Invalid country provided: " + cc);
            }
        }
    }

    @Override
    public int maskData(StringBuilder builder, char maskChar, int startPos, int buffLength) {
        Map<Character, Integer> potential = countriesToCheck.get(builder.charAt(startPos));
        if (potential != null) {
            Integer ibanLength = potential.get(builder.charAt(startPos + 1));
            if (shouldMask(builder, buffLength, startPos, ibanLength)) {
                builder.replace(startPos + 3, startPos + ibanLength - 4,
                        StringUtils.repeat(maskChar, ibanLength - 7));
                return startPos + ibanLength;
            }
        }

        return startPos;
    }

    private boolean shouldMask(StringBuilder builder, int buffLength, int start, Integer ibanLength) {
        if (ibanLength == null) {
            return false;
        }

        if (builder.length() < start + ibanLength) {
            return false;
        }

        if (!Character.isDigit(builder.charAt(start + 2))) {
            return false;
        }

        if (buffLength == start + ibanLength
                || LogMasker.indexOfNextDelimiter(builder, start, buffLength) == start + ibanLength) {
            return true;
        }

        return false;
    }

    private static final Map<Character, Map<Character, Integer>> buildCountriesDictionary(Object... args) {
        Map<Character, Map<Character, Integer>> countries = new HashMap<>();
        for (int count = 0; count < args.length; count += 2) {
            Character firstChar = ((String)args[count]).charAt(0);
            Map<Character, Integer> lengths = countries.get(firstChar);
            if (lengths == null) {
                lengths = new HashMap<>();
                countries.put(firstChar, lengths);
            }
            lengths.put(((String)args[count]).charAt(1), (Integer)args[count + 1]);
        }

        return countries;
    }

    private static final Map<Character, Map<Character, Integer>> COUNTRIES_DICTIONARY
            = buildCountriesDictionary("AL", 28,
            "AD", 24,
            "AT", 20,
            "AZ", 28,
            "BH", 22,
            "BY", 28,
            "BE", 16,
            "BA", 20,
            "BR", 29,
            "BG", 22,
            "CR", 22,
            "HR", 21,
            "CY", 28,
            "CZ", 24,
            "DK", 18,
            "DO", 28,
            "EG", 29,
            "SV", 28,
            "EE", 20,
            "FO", 18,
            "FI", 18,
            "FR", 27,
            "GE", 22,
            "DE", 22,
            "GI", 23,
            "GR", 27,
            "GL", 18,
            "GT", 28,
            "VA", 22,
            "HU", 28,
            "IS", 28,
            "IQ", 23,
            "IE", 22,
            "IL", 23,
            "IT", 27,
            "JO", 30,
            "KZ", 20,
            "XK", 20,
            "KW", 30,
            "LV", 21,
            "LB", 28,
            "LY", 25,
            "LI", 21,
            "LT", 20,
            "LU", 20,
            "MT", 31,
            "MR", 27,
            "MU", 30,
            "MD", 24,
            "MC", 27,
            "ME", 22,
            "NL", 18,
            "MK", 19,
            "NO", 15,
            "PK", 24,
            "PS", 29,
            "PL", 28,
            "PT", 25,
            "QA", 29,
            "RO", 24,
            "LC", 32,
            "SM", 27,
            "ST", 25,
            "SA", 24,
            "RS", 22,
            "SC", 31,
            "SK", 24,
            "SI", 19,
            "ES", 24,
            "SD", 18,
            "SE", 24,
            "CH", 21,
            "TL", 23,
            "TN", 24,
            "TR", 26,
            "UA", 29,
            "AE", 23,
            "GB", 22,
            "VG", 24);
}
