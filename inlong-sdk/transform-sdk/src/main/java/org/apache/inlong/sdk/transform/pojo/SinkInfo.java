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

package org.apache.inlong.sdk.transform.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * SinkInfo
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @Type(value = CsvSinkInfo.class, name = SinkInfo.CSV),
        @Type(value = KvSinkInfo.class, name = SinkInfo.KV),
})
@SuperBuilder
@Data
public abstract class SinkInfo {

    public static final String CSV = "csv";
    public static final String KV = "kv";
    public static final String ES_MAP = "es_map";
    public static final String PARQUET = "parquet";
    public static final String PB = "pb";

    @JsonIgnore
    private String type;

    @JsonProperty("charset")
    private String charset;

    public SinkInfo(
            String type,
            @JsonProperty("charset") String charset) {
        this.type = checkNotNull(type);
        this.charset = Optional.ofNullable(charset).orElse("UTF-8");
    }

    /**
     * get type
     * @return the type
     */
    @JsonIgnore
    public String getType() {
        return type;
    }

    /**
     * set type
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * get charset
     * @return the charset
     */
    @JsonProperty("charset")
    public String getCharset() {
        return charset;
    }

    /**
     * set charset
     * @param charset the charset to set
     */
    public void setCharset(String charset) {
        this.charset = charset;
    }

}
