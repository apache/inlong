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

package org.apache.inlong.sort.protocol.node.format;

import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_KV_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_QUOTE_CHARACTER;

@JsonTypeName("kvFormat")
@Data
public class KvFormat implements Format {

    private static final String IDENTIFIER = "inlong-kv";

    @JsonProperty(value = "entryDelimiter", defaultValue = "&")
    private final String entryDelimiter;

    @JsonProperty(value = "kvDelimiter", defaultValue = "=")
    private final String kvDelimiter;

    @JsonProperty(value = "ignoreParseErrors", defaultValue = "false")
    @Nullable
    private final String ignoreParseErrors;

    @JsonProperty(value = "escapeChar")
    @Nullable
    private final String escapeChar;

    @JsonProperty(value = "charset")
    @Nullable
    private final String charset;

    @JsonProperty(value = "nullLiteral")
    @Nullable
    private final String nullLiteral;

    @JsonProperty(value = "quoteCharacter")
    @Nullable
    private final String quoteCharacter;

    @JsonCreator
    public KvFormat(@JsonProperty(value = "entryDelimiter") String entryDelimiter,
            @JsonProperty(value = "kvDelimiter") String kvDelimiter,
            @Nullable @JsonProperty(value = "escapeChar") String escapeChar,
            @Nullable @JsonProperty(value = "ignoreParseErrors", defaultValue = "false") String ignoreParseErrors,
            @Nullable @JsonProperty(value = "charset") String charset,
            @Nullable @JsonProperty(value = "nullLiteral") String nullLiteral,
            @Nullable @JsonProperty(value = "quoteCharacter") String quoteCharacter) {
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.escapeChar = escapeChar;
        this.ignoreParseErrors = ignoreParseErrors;
        this.charset = charset;
        this.nullLiteral = nullLiteral;
        this.quoteCharacter = quoteCharacter;
    }

    public KvFormat() {
        this("&", "=", null, "false", null, null, null);
    }

    @Override
    @JsonIgnore
    public String getFormat() {
        return IDENTIFIER;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Map<String, String> generateOptions() {
        Map<String, String> options = new HashMap<>(16);

        options.put("format", getFormat());
        options.put(FORMAT_KV_DELIMITER, this.kvDelimiter);
        options.put(FORMAT_KV_ENTRY_DELIMITER, this.entryDelimiter);

        if (ObjectUtils.isNotEmpty(this.charset)) {
            options.put(FORMAT_CHARSET, this.charset);
        }
        if (ObjectUtils.isNotEmpty(this.nullLiteral)) {
            options.put(FORMAT_NULL_LITERAL, this.nullLiteral);
        }
        if (ObjectUtils.isNotEmpty(this.quoteCharacter)) {
            options.put(FORMAT_QUOTE_CHARACTER, this.quoteCharacter);
        }
        if (ObjectUtils.isNotEmpty(this.escapeChar)) {
            options.put(FORMAT_ESCAPE_CHARACTER, this.escapeChar);
        }
        if (ObjectUtils.isNotEmpty(this.ignoreParseErrors)) {
            options.put(FORMAT_IGNORE_ERRORS, this.ignoreParseErrors);
        }

        return options;
    }

}
