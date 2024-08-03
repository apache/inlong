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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * TransformConfig
 */
public class TransformConfig {

    @JsonProperty("transformSql")
    private String transformSql;

    @JsonProperty("configuration")
    private Map<String, Object> configuration;

    @JsonCreator
    public TransformConfig(@JsonProperty("transformSql") String transformSql) {
        this(transformSql, ImmutableMap.of());
    }

    @JsonCreator
    public TransformConfig(@JsonProperty("transformSql") String transformSql,
            @JsonProperty("configuration") Map<String, Object> configuration) {
        this.transformSql = Preconditions.checkNotNull(transformSql, "transform sql should not be null");
        this.configuration = configuration;
    }

    /**
     * get transformSql
     * @return the transformSql
     */
    @JsonProperty("transformSql")
    public String getTransformSql() {
        return transformSql;
    }

    @JsonProperty("configuration")
    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    /**
     * set transformSql
     * @param transformSql the transformSql to set
     */
    public void setTransformSql(String transformSql) {
        this.transformSql = transformSql;
    }

}
