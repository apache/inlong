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

package org.apache.inlong.manager.pojo.cluster;

import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * Extended params, will be saved as JSON string
 */
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ApiModel("Inlong cluster tag ext param info")
public class InlongClusterTagExtParam implements Serializable {

    private static final Gson GSON = new Gson();

    @ApiModelProperty(value = "The compression type used for dataproxy and sort side data transmission to reduce the network IO overhead")
    private String inlongCompressType = "NONE";

    /**
     * Pack extended attributes into ExtParams
     *
     * @param request the request
     * @return the packed extParams
     */
    public static String packExtParams(ClusterTagRequest request) {
        InlongClusterTagExtParam extParam = CommonBeanUtils.copyProperties(request, InlongClusterTagExtParam::new,
                true);
        JsonObject obj = GSON.fromJson(JsonUtils.toJsonString(extParam), JsonObject.class);
        if (StringUtils.isBlank(request.getExtParams())) {
            return obj.toString();
        }
        JsonObject existObj = GSON.fromJson(request.getExtParams(), JsonObject.class);
        for (String key : obj.keySet()) {
            JsonElement child = obj.get(key);
            if (child.isJsonNull()) {
                continue;
            } else if (child.isJsonPrimitive()) {
                JsonPrimitive jsonPrimitive = child.getAsJsonPrimitive();
                if (jsonPrimitive.isBoolean()) {
                    existObj.addProperty(key, child.getAsBoolean());
                } else if (jsonPrimitive.isNumber()) {
                    existObj.addProperty(key, child.getAsInt());
                } else {
                    existObj.addProperty(key, child.getAsString());
                }
            } else {
                existObj.addProperty(key, child.toString());
            }
        }
        return existObj.toString();
    }

    /**
     * Unpack extended attributes from {@link ClusterTagResponse}, will remove target attributes from it.
     *
     * @param extParams the extParams value load from db
     * @param targetObject the targetObject with to fill up
     */
    public static void unpackExtParams(
            String extParams,
            Object targetObject) {
        if (StringUtils.isNotBlank(extParams)) {
            InlongClusterTagExtParam inlongClusterTagExtParam =
                    JsonUtils.parseObject(extParams, InlongClusterTagExtParam.class);
            if (inlongClusterTagExtParam != null) {
                CommonBeanUtils.copyProperties(inlongClusterTagExtParam, targetObject, true);
            }
        }
    }

    /**
     * Expand extParam filed, and fill in {@link ClusterTagResponse}
     *
     * @param clusterTagResponse the clusterTagResponse need to filled
     */
    public static void unpackExtParams(ClusterTagResponse clusterTagResponse) {
        unpackExtParams(clusterTagResponse.getExtParams(), clusterTagResponse);
    }
}
