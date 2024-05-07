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
 * PbSourceInfo
 * 
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PbSourceInfo extends SourceInfo {

    private String protoDescription;
    private String rootMessageType;
    private String rowsNodePath;

    @JsonCreator
    public PbSourceInfo(
            @JsonProperty("charset") String charset,
            @JsonProperty("protoDescription") String protoDescription,
            @JsonProperty("rootMessageType") String rootMessageType,
            @JsonProperty("rowsNodePath") String rowsNodePath) {
        super(charset);
        this.protoDescription = protoDescription;
        this.rootMessageType = rootMessageType;
        this.rowsNodePath = rowsNodePath;
    }

    /**
     * get protoDescription
     * @return the protoDescription
     */
    @JsonProperty("protoDescription")
    public String getProtoDescription() {
        return protoDescription;
    }

    /**
     * set protoDescription
     * @param protoDescription the protoDescription to set
     */
    public void setProtoDescription(String protoDescription) {
        this.protoDescription = protoDescription;
    }

    /**
     * get rootMessageType
     * @return the rootMessageType
     */
    @JsonProperty("rootMessageType")
    public String getRootMessageType() {
        return rootMessageType;
    }

    /**
     * set rootMessageType
     * @param rootMessageType the rootMessageType to set
     */
    public void setRootMessageType(String rootMessageType) {
        this.rootMessageType = rootMessageType;
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
