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

package org.apache.inlong.sort.formats.json;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser.Feature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import static org.apache.inlong.sort.formats.json.JsonFormatOptions.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.ALLOW_COMMENTS;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.ALLOW_MISSING_VALUES;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.ALLOW_NON_NUMERIC_NUMBERS;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.ALLOW_NUMERIC_LEADING_ZEROS;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.ALLOW_SINGLE_QUOTES;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.ALLOW_TRAILING_COMMA;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.ALLOW_UNQUOTED_CONTROL_CHARS;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.ALLOW_UNQUOTED_FIELD_NAMES;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.ALLOW_YAML_COMMENTS;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.AUTO_CLOSE_SOURCE;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.IGNORE_UNDEFINED;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.INCLUDE_SOURCE_IN_LOCATION;
import static org.apache.inlong.sort.formats.json.JsonFormatOptions.STRICT_DUPLICATE_DETECTION;

/**
 * The utils of Json.
 */
public class JsonFormatUtils {

    public static ObjectMapper createObjectMapper(ReadableConfig config) {
        ObjectMapper mapper = new ObjectMapper();

        mapper.configure(Feature.AUTO_CLOSE_SOURCE, config.get(AUTO_CLOSE_SOURCE));
        mapper.configure(Feature.ALLOW_COMMENTS, config.get(ALLOW_COMMENTS));
        mapper.configure(Feature.ALLOW_YAML_COMMENTS, config.get(ALLOW_YAML_COMMENTS));
        mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, config.get(ALLOW_UNQUOTED_FIELD_NAMES));
        mapper.configure(Feature.ALLOW_SINGLE_QUOTES, config.get(ALLOW_SINGLE_QUOTES));
        mapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, config.get(ALLOW_UNQUOTED_CONTROL_CHARS));
        mapper.configure(Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
                config.get(ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER));
        mapper.configure(Feature.ALLOW_NUMERIC_LEADING_ZEROS, config.get(ALLOW_NUMERIC_LEADING_ZEROS));
        mapper.configure(Feature.ALLOW_NON_NUMERIC_NUMBERS, config.get(ALLOW_NON_NUMERIC_NUMBERS));
        mapper.configure(Feature.ALLOW_MISSING_VALUES, config.get(ALLOW_MISSING_VALUES));
        mapper.configure(Feature.ALLOW_TRAILING_COMMA, config.get(ALLOW_TRAILING_COMMA));
        mapper.configure(Feature.STRICT_DUPLICATE_DETECTION, config.get(STRICT_DUPLICATE_DETECTION));
        mapper.configure(Feature.IGNORE_UNDEFINED, config.get(IGNORE_UNDEFINED));
        mapper.configure(Feature.INCLUDE_SOURCE_IN_LOCATION, config.get(INCLUDE_SOURCE_IN_LOCATION));

        return mapper;
    }
}
