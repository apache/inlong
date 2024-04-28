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

package org.apache.inlong.manager.pojo.cluster.agent;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.module.ModuleHistory;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Agent cluster node info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Agent cluster node info")
public class AgentClusterNodeDTO {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentClusterNodeDTO.class);

    @ApiModelProperty(value = "Agent group name")
    private String agentGroup;

    @ApiModelProperty(value = "Module id list")
    @Default
    private List<Integer> moduleIdList = new ArrayList<>();

    @ApiModelProperty(value = "Agent restart time")
    private Integer agentRestartTime = 0;

    @ApiModelProperty(value = "Install restart time")
    private Integer installRestartTime = 0;

    @ApiModelProperty("History list of module")
    @Default
    private List<ModuleHistory> moduleHistoryList = new ArrayList<>();

    /**
     * Get the dto instance from the request
     */
    public static AgentClusterNodeDTO getFromRequest(AgentClusterNodeRequest request, String extParams) {
        AgentClusterNodeDTO dto;
        if (!StringUtils.isNotBlank(extParams)) {
            return CommonBeanUtils.copyProperties(request, AgentClusterNodeDTO::new, true);
        }
        dto = AgentClusterNodeDTO.getFromJson(extParams);
        if (!CollectionUtils.isEqualCollection(request.getModuleIdList(), dto.getModuleIdList())) {
            request.setModuleHistoryList(dto.getModuleHistoryList());
            List<ModuleHistory> moduleHistoryList = request.getModuleHistoryList();
            if (moduleHistoryList.size() > 10) {
                moduleHistoryList.remove(moduleHistoryList.size() - 1);
            }
            ModuleHistory moduleHistory = ModuleHistory.builder()
                    .moduleIdList(dto.getModuleIdList())
                    .modifier(request.getCurrentUser())
                    .modifyTime(new Date())
                    .build();
            moduleHistoryList.add(0, moduleHistory);
            dto.setModuleHistoryList(moduleHistoryList);
        }
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static AgentClusterNodeDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, AgentClusterNodeDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT,
                    ErrorCodeEnum.CLUSTER_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }
}
