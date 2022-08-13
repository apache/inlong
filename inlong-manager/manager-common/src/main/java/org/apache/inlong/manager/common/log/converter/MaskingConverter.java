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

package org.apache.inlong.manager.common.log.converter;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.log.converter.masker.CardNumberMasker;
import org.apache.inlong.manager.common.log.converter.masker.EmailMasker;
import org.apache.inlong.manager.common.log.converter.masker.IPMasker;
import org.apache.inlong.manager.common.log.converter.masker.IbanMasker;
import org.apache.inlong.manager.common.log.converter.masker.LogMasker;
import org.apache.inlong.manager.common.log.converter.masker.PasswordMasker;
import org.apache.inlong.manager.common.log.converter.masker.SequentialLogMasker;
import org.reflections.Reflections;

public class MaskingConverter {
    private static final List<Character> STOP_CHARACTERS = Arrays.asList('\'', '"', '@', '>');

    private static final Map<String, LogMasker> OPTIONS_TO_MASKER = initializeDefaultMaskers();

    private static final List<LogMasker> ALL_MASKERS = OPTIONS_TO_MASKER.entrySet().stream()
            .map(Map.Entry::getValue).collect(Collectors.toList());

    private List<LogMasker> maskers = new ArrayList<>();

    private List<SequentialLogMasker> sequentialMaskers = new ArrayList<>();

    public void init(List<String> options) {
        if (options != null) {
            for (String option : options) {
                if (StringUtils.startsWith(option, "custom")) {
                    maskers.addAll(buildCustomMaskersList(option));
                    sequentialMaskers.addAll(buildCustomSequentialMaskersList(option));
                } else if (option.equalsIgnoreCase("all")) {
                    maskers.addAll(ALL_MASKERS);
                } else {
                    LogMasker masker = getMaskerFromOptions(option);
                    maskers.add(masker);
                }
            }
        }

        if (maskers.isEmpty()) {
            maskers.addAll(ALL_MASKERS);
        }
    }

    public void setMaskers(List<LogMasker> maskers) {
        this.maskers.clear();
        this.maskers.addAll(maskers);
    }

    private LogMasker getMaskerFromOptions(String option) {
        String args = null;
        int idxOfArgsSeparator = StringUtils.indexOf(option, ':');
        if (idxOfArgsSeparator > 0) {
            args = StringUtils.substring(option, idxOfArgsSeparator + 1);
            option = StringUtils.substring(option, 0, idxOfArgsSeparator);
        }
        LogMasker masker = OPTIONS_TO_MASKER.get(option);
        if (masker == null) {
            throw new ExceptionInInitializerError("Invalid option provided: " + option);
        }
        if (args != null) {
            masker.initialize(args);
        }

        return masker;
    }

    private static List<LogMasker> buildCustomMaskersList(String params) {
        int idxOfArgsSeparator = StringUtils.indexOf(params, ':');
        if (idxOfArgsSeparator < 0) {
            return Collections.emptyList();
        }
        List<LogMasker> maskers = new ArrayList<>();

        String args = StringUtils.substring(params, idxOfArgsSeparator + 1);
        String[] packages = StringUtils.split(args, '|');
        for (String pack:packages) {
            Reflections reflections = new Reflections(pack);
            initializeCustomLogMaskers(maskers, reflections);
        }

        return maskers;
    }

    private static void initializeCustomLogMaskers(List<LogMasker> maskers, Reflections reflections) {
        Set<Class<? extends LogMasker>> allClasses = reflections.getSubTypesOf(LogMasker.class);
        for (Class<? extends LogMasker> clazz:allClasses) {
            try {
                Constructor<? extends LogMasker> maskerConstructor = clazz.getConstructor();
                LogMasker masker = maskerConstructor.newInstance();
                maskers.add(masker);
            } catch (Exception e) {
                System.err.println(e);
            }
        }
    }

    private static List<SequentialLogMasker> buildCustomSequentialMaskersList(String params) {
        int idxOfArgsSeparator = StringUtils.indexOf(params, ':');
        if (idxOfArgsSeparator < 0) {
            return Collections.emptyList();
        }
        List<SequentialLogMasker> maskers = new ArrayList<>();

        String args = StringUtils.substring(params, idxOfArgsSeparator + 1);
        String[] packages = StringUtils.split(args, '|');
        for (String pack:packages) {
            Reflections reflections = new Reflections(pack);
            initializeCustomSequentialLogMaskers(maskers, reflections);
        }

        return maskers;
    }

    private static void initializeCustomSequentialLogMaskers(List<SequentialLogMasker> maskers,
            Reflections reflections) {
        Set<Class<? extends SequentialLogMasker>> allClasses = reflections.getSubTypesOf(SequentialLogMasker.class);
        for (Class<? extends SequentialLogMasker> clazz:allClasses) {
            try {
                Constructor<? extends SequentialLogMasker> maskerConstructor = clazz.getConstructor();
                SequentialLogMasker masker = maskerConstructor.newInstance();
                maskers.add(masker);
            } catch (Exception e) {
                System.err.println(e);
            }
        }
    }

    public void mask(StringBuilder stringBuilder) {
        boolean maskedThisCharacter;
        int pos;
        int newPos;
        int length = stringBuilder.length();
        for (pos = 0; pos < length; pos++) {
            maskedThisCharacter = false;
            for (LogMasker masker : maskers) {
                newPos = masker.maskData(stringBuilder, '*', pos, length);
                maskedThisCharacter = newPos != pos;
                if (maskedThisCharacter) {
                    length = stringBuilder.length();
                    maskedThisCharacter = true;
                    break;
                }
            }
            if (!maskedThisCharacter) {
                while (pos < length && !(Character.isWhitespace(stringBuilder.charAt(pos))
                        || STOP_CHARACTERS.contains(stringBuilder.charAt(pos)))) {
                    pos++;
                }
            }
        }

        maskSequential(stringBuilder);
    }

    private void maskSequential(StringBuilder builder) {
        for (SequentialLogMasker masker: sequentialMaskers) {
            try {
                masker.mask(builder, '*');
            } catch (Exception e) {
                System.err.println("Error applying masker " + masker + ". Error: " + e.getMessage());
            }
        }
    }

    private static Map<String, LogMasker> initializeDefaultMaskers() {
        Map<String, LogMasker> maskers = new HashMap<>();
        maskers.put("email", new EmailMasker());
        maskers.put("pass", new PasswordMasker());
        maskers.put("ip", new IPMasker());
        maskers.put("card", new CardNumberMasker());
        maskers.put("iban", new IbanMasker());

        return maskers;
    }
}
