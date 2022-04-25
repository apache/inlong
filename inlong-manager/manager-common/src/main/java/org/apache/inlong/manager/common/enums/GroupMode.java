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

package org.apache.inlong.manager.common.enums;

import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;

import java.util.List;

/**
 * Mode of inlong group
 */
public enum GroupMode {
    /**
     * Normal group init with all components in Inlong Cluster
     * StreamSource -> Agent/SDK -> DataProxy -> Cache -> Sort -> StreamSink
     */
    NORMAL("normal"),

    /**
     * Light group init with sort in Inlong Cluster
     * StreamSource -> Sort -> StreamSink
     */
    LIGHT("light");

    @Getter
    private String mode;

    GroupMode(String mode) {
        this.mode = mode;
    }

    public static GroupMode forMode(String mode) {
        for (GroupMode groupMode : values()) {
            if (groupMode.getMode().equals(mode)) {
                return groupMode;
            }
        }
        throw new IllegalArgumentException(String.format("Unsupported group mode=%s for Inlong", mode));
    }

    public static GroupMode parseGroupMode(InlongGroupInfo groupInfo) {
        List<InlongGroupExtInfo> extInfos = groupInfo.getExtList();
        if (CollectionUtils.isEmpty(extInfos)) {
            return GroupMode.NORMAL;
        }
        for (InlongGroupExtInfo extInfo : extInfos) {
            if (InlongGroupSettings.GROUP_MODE.equals(extInfo.getKeyName())) {
                return GroupMode.forMode(extInfo.getKeyValue());
            }
        }
        return GroupMode.NORMAL;
    }
}
