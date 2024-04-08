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

package org.apache.inlong.manager.pojo.node.es;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ElasticsearchQueryInfo {

    /**
     *  Bool query value.
     */
    private QueryBool bool;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QueryBool {

        /**
         * Must query value.
         */
        private List<QueryTerm> must;

        /**
         *  Use the boost parameter to boost search results.
         */
        private double boost;

        /**
         * adjust pure negative.
         */
        @SerializedName("adjust_pure_negative")
        private boolean adjustPureNegative;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QueryTerm {

        /**
         * Term query value.
         */
        private Map<String, TermValue> term;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TermValue {

        /**
         * Term query value.
         */
        private String value;

        /**
         *  Use the boost parameter to boost search results.
         */
        private double boost;
    }
}
