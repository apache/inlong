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

package org.apache.inlong.manager.pojo.sink.es;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ElasticsearchIndexMappingInfo {

    /**
     *  mapping.
     */
    private IndexMappings mappings;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IndexMappings {

        /**
         * properties.
         */
        private Map<String, IndexField> properties;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IndexField {

        /**
         *  Index field type.
         */
        private String type;

        /**
         * Index text type field analyzer.
         */
        private String analyzer;

        /**
         * Index text type field search analyzer.
         */
        @SerializedName("search_analyzer")
        private String searchAnalyzer;

        /**
         * Index date type field search format.
         */
        private String format;

        /**
         * Index scaled_float type scaling factor.
         */
        @SerializedName("scaling_factor")
        private String scalingFactor;
    }
}
