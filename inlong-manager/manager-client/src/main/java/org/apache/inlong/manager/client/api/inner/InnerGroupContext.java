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

package org.apache.inlong.manager.client.api.inner;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.util.AssertUtil;
import org.apache.inlong.manager.common.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamApproveRequest;

@Data
@NoArgsConstructor
public class InnerGroupContext {

    private InlongGroupRequest groupInfo;

    private Map<String, InnerStreamContext> streamContextMap;

    private Map<String, InlongStream> streamMap;

    private Pair<InlongGroupApproveRequest, List<InlongStreamApproveRequest>> initMsg;

    public String getGroupId() {
        AssertUtil.notNull(groupInfo, "InlongGroupRequest is not init");
        return groupInfo.getInlongGroupId();
    }

    public void setStreamContext(InnerStreamContext streamContext) {
        AssertUtil.isTrue(streamContext != null && streamContext.getStreamInfo() != null,
                "StreamContext should not be null");
        if (MapUtils.isEmpty(streamContextMap)) {
            streamContextMap = Maps.newHashMap();
        }
        streamContextMap.put(streamContext.getStreamInfo().getName(), streamContext);
    }

    public void setStream(InlongStream stream) {
        AssertUtil.isTrue(stream != null,
                "Stream should not be null");
        if (MapUtils.isEmpty(streamMap)) {
            streamMap = Maps.newHashMap();
        }
        streamMap.put(stream.getName(), stream);
    }
}
