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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * CsvSourceInfo
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuperBuilder
public class CsvSourceInfo extends SourceInfo {

    private Character delimiter;
    private Character escapeChar;
    private List<FieldInfo> fields;

    @JsonCreator
    public CsvSourceInfo(
            @JsonProperty("charset") String charset,
            @JsonProperty("delimiter") Character delimiter,
            @JsonProperty("escapeChar") Character escapeChar,
            @JsonProperty("fields") List<FieldInfo> fields) {
        super(charset);
        this.delimiter = delimiter;
        this.escapeChar = escapeChar;
        if (fields != null) {
            this.fields = fields;
        } else {
            this.fields = new ArrayList<>();
        }
    }

    /**
     * get delimiter
     * @return the delimiter
     */
    @JsonProperty("delimiter")
    public Character getDelimiter() {
        return delimiter;
    }

    /**
     * set delimiter
     * @param delimiter the delimiter to set
     */
    public void setDelimiter(Character delimiter) {
        this.delimiter = delimiter;
    }

    /**
     * get escapeChar
     * @return the escapeChar
     */
    @JsonProperty("escapeChar")
    public Character getEscapeChar() {
        return escapeChar;
    }

    /**
     * set escapeChar
     * @param escapeChar the escapeChar to set
     */
    public void setEscapeChar(Character escapeChar) {
        this.escapeChar = escapeChar;
    }

    /**
     * get fields
     * @return the fields
     */
    @JsonProperty("fields")
    public List<FieldInfo> getFields() {
        return fields;
    }

    /**
     * set fields
     * @param fields the fields to set
     */
    public void setFields(List<FieldInfo> fields) {
        this.fields = fields;
    }

}
