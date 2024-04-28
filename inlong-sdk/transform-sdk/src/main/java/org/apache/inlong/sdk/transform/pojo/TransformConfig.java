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
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * TransformConfig
 */
public class TransformConfig {

    @JsonProperty("sourceInfo")
    private SourceInfo sourceInfo;
    @JsonProperty("sinkInfo")
    private SinkInfo sinkInfo;
    @JsonProperty("transformSql")
    private String transformSql;

    @JsonCreator
    public TransformConfig(
            @JsonProperty("sourceInfo") SourceInfo sourceInfo,
            @JsonProperty("sinkInfo") SinkInfo sinkInfo,
            @JsonProperty("transformSql") String transformSql) {
        this.sourceInfo = sourceInfo;
        this.sinkInfo = sinkInfo;
        this.transformSql = transformSql;
    }

    /**
     * get sourceInfo
     * @return the sourceInfo
     */
    @JsonProperty("sourceInfo")
    public SourceInfo getSourceInfo() {
        return sourceInfo;
    }

    /**
     * set sourceInfo
     * @param sourceInfo the sourceInfo to set
     */
    public void setSourceInfo(SourceInfo sourceInfo) {
        this.sourceInfo = sourceInfo;
    }

    /**
     * get sinkInfo
     * @return the sinkInfo
     */
    @JsonProperty("sinkInfo")
    public SinkInfo getSinkInfo() {
        return sinkInfo;
    }

    /**
     * set sinkInfo
     * @param sinkInfo the sinkInfo to set
     */
    public void setSinkInfo(SinkInfo sinkInfo) {
        this.sinkInfo = sinkInfo;
    }

    /**
     * get transformSql
     * @return the transformSql
     */
    @JsonProperty("transformSql")
    public String getTransformSql() {
        return transformSql;
    }

    /**
     * set transformSql
     * @param transformSql the transformSql to set
     */
    public void setTransformSql(String transformSql) {
        this.transformSql = transformSql;
    }

}
