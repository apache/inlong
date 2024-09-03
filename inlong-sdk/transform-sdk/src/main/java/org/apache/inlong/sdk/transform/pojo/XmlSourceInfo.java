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

/**
 * XmlSourceInfo
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class XmlSourceInfo extends SourceInfo {

    private String rowsNodePath;

    @JsonCreator
    public XmlSourceInfo(
            @JsonProperty("charset") String charset,
            @JsonProperty("rowsNodePath") String rowsNodePath) {
        super(charset);
        this.rowsNodePath = rowsNodePath;
    }

    /**
     * get rowsNodePath
     * @return the rowsNodePath
     */
    @JsonProperty("rowsNodePath")
    public String getRowsNodePath() {
        return rowsNodePath;
    }

    /**
     * set rowsNodePath
     * @param rowsNodePath the rowsNodePath to set
     */
    public void setRowsNodePath(String rowsNodePath) {
        this.rowsNodePath = rowsNodePath;
    }
}
