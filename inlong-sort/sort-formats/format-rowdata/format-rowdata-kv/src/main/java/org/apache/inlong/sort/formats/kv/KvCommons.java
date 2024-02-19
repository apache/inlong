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

package org.apache.inlong.sort.formats.kv;

import org.apache.inlong.sort.formats.base.TextFormatOptions;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

/**
 * Commons values and methods for KV format.
 */
public class KvCommons {

    static void validateFormatOptions(ReadableConfig tableOptions) {

        validateCharacterVal(tableOptions, TextFormatOptions.FIELD_DELIMITER, true);
        validateCharacterVal(tableOptions, TextFormatOptions.KV_DELIMITER, true);
        validateCharacterVal(tableOptions, TextFormatOptions.KV_ENTRY_DELIMITER, true);
        validateCharacterVal(tableOptions, TextFormatOptions.QUOTE_CHARACTER);
        validateCharacterVal(tableOptions, TextFormatOptions.ESCAPE_CHARACTER);
    }

    private static void validateCharacterVal(
            ReadableConfig tableOptions,
            ConfigOption<String> option) {
        validateCharacterVal(tableOptions, option, false);
    }

    private static void validateCharacterVal(
            ReadableConfig tableOptions,
            ConfigOption<String> option,
            boolean unescape) {
        if (!tableOptions.getOptional(option).isPresent()) {
            return;
        }

        final String value = unescape
                ? StringEscapeUtils.unescapeJava(tableOptions.get(option))
                : tableOptions.get(option);

        if (value.length() != 1) {
            throw new ValidationException(
                    String.format(
                            "Option '%s.%s' must be a String with single character, but was: %s",
                            KvFormatFactory.IDENTIFIER, option.key(), tableOptions.get(option)));
        }
    }

}
