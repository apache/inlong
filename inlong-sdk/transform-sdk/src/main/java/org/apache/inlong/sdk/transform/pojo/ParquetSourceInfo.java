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
 * ParquetSourceInfo
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ParquetSourceInfo extends SourceInfo {

    private String parquetSchema;
    private String rootMessageLabel;
    private String childMessagePath;

    @JsonCreator
    public ParquetSourceInfo(
            @JsonProperty("charset") String charset,
            @JsonProperty("parquetSchema") String parquetSchema,
            @JsonProperty("rootMessageLabel") String rootMessageLabel,
            @JsonProperty("childMessagePath") String childMessagePath) {
        super(charset);
        this.parquetSchema = parquetSchema;
        this.rootMessageLabel = rootMessageLabel;
        this.childMessagePath = childMessagePath;
    }

    /**
     * get ParquetSchema
     *
     * @return the protoDescription
     */
    @JsonProperty("ParquetSchema")
    public String getParquetSchema() {
        return parquetSchema;
    }

    /**
     * set ParquetSchema
     *
     * @param parquetSchema the parquetSchema to set
     */
    public void setParquetSchema(String parquetSchema) {
        this.parquetSchema = parquetSchema;
    }

    /**
     * get rootMessageLabel
     *
     * @return the rootMessageLabel
     */
    @JsonProperty("rootMessageLabel")
    public String getRootMessageLabel() {
        return rootMessageLabel;
    }

    /**
     * set rootMessageLabel
     *
     * @param rootMessageLabel the rootMessageLabel to set
     */
    public void setRootMessageLabel(String rootMessageLabel) {
        this.rootMessageLabel = rootMessageLabel;
    }

    /**
     * get childMessagePath
     *
     * @return the childMessagePath
     */
    @JsonProperty("childMessagePath")
    public String getChildMessagePath() {
        return childMessagePath;
    }

    /**
     * set childMessagePath
     *
     * @param childMessagePath the childMessagePath to set
     */
    public void setChildMessagePath(String childMessagePath) {
        this.childMessagePath = childMessagePath;
    }

}
