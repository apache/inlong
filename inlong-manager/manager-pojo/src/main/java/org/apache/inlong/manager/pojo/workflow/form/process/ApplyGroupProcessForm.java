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

package org.apache.inlong.manager.pojo.workflow.form.process;

import org.apache.inlong.manager.common.exceptions.FormValidateException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamBriefInfo;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Apply inlong group process form
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ApplyGroupProcessForm extends BaseProcessForm {

    public static final String FORM_NAME = "ApplyGroupProcessForm";

    @ApiModelProperty(value = "Inlong group info")
    private InlongGroupInfo groupInfo;

    @ApiModelProperty(value = "All inlong stream info under the inlong group, including the sink info")
    private List<InlongStreamBriefInfo> streamInfoList;

    @ApiModelProperty(value = "Inlong group full info list")
    private List<GroupFullInfo> groupFullInfoList;

    @Override
    public void validate() throws FormValidateException {
        Preconditions.expectTrue(groupInfo != null || CollectionUtils.isNotEmpty(groupFullInfoList),
                "inlong group info is empty");
    }

    @Override
    public String getFormName() {
        return FORM_NAME;
    }

    @Override
    public String getInlongGroupId() {
        if (groupInfo != null) {
            return groupInfo.getInlongGroupId();
        }
        List<String> groupIdList = groupFullInfoList.stream().map(v -> {
            InlongGroupInfo groupInfo = v.getGroupInfo();
            return groupInfo.getInlongGroupId();
        }).collect(Collectors.toList());
        return Joiner.on(",").join(groupIdList);
    }

    @Override
    public List<Map<String, Object>> showInList() {
        List<Map<String, Object>> showList = new ArrayList<>();
        if (groupInfo != null) {
            addShowInfo(groupInfo, showList);
        }
        if (CollectionUtils.isNotEmpty(groupFullInfoList)) {
            groupFullInfoList.forEach(groupFullInfo -> {
                addShowInfo(groupFullInfo.getGroupInfo(), showList);
            });
        }
        return showList;
    }

    private void addShowInfo(InlongGroupInfo groupInfo, List<Map<String, Object>> showList) {
        Map<String, Object> show = Maps.newHashMap();
        show.put("inlongGroupId", groupInfo.getInlongGroupId());
        show.put("inlongGroupMode", groupInfo.getInlongGroupMode());
        showList.add(show);
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GroupFullInfo {

        private InlongGroupInfo groupInfo;

        private List<InlongStreamBriefInfo> streamInfoList;

    }
}
