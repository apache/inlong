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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@SuperBuilder
@Data
public class MapSinkInfo extends SinkInfo {

    private List<FieldInfo> fields;

    public MapSinkInfo(
            @JsonProperty("charset") String charset,
            @JsonProperty("fields") List<FieldInfo> fields) {
        super(SinkInfo.ES_MAP, charset);
        if (CollectionUtils.isEmpty(fields)) {
            throw new IllegalArgumentException("failed to init map sink info, fieldInfos is empty");
        }
        this.fields = fields;
    }
}
