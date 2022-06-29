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

package org.apache.inlong.manager.service.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.ProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.StreamResourceProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.EventSelector;

@Slf4j
public class PulsarTopicCreateSelector implements EventSelector {

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        if (!(processForm instanceof StreamResourceProcessForm)) {
            return false;
        }
        StreamResourceProcessForm streamResourceProcessForm = (StreamResourceProcessForm) processForm;
        GroupOperateType operateType = streamResourceProcessForm.getGroupOperateType();
        if (operateType != GroupOperateType.INIT) {
            return false;
        }
        MQType mqType = MQType.forType(streamResourceProcessForm.getGroupInfo().getMqType());
        String groupId = streamResourceProcessForm.getGroupInfo().getInlongGroupId();
        String streamId = streamResourceProcessForm.getStreamInfo().getInlongStreamId();
        if (mqType == MQType.PULSAR || mqType == MQType.TDMQ_PULSAR) {
            InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) streamResourceProcessForm.getGroupInfo();
            boolean enable = InlongConstants.ENABLE_CREATE_RESOURCE.equals(pulsarInfo.getEnableCreateResource());
            if (enable) {
                log.info("need to create pulsar topic as the createResource was true for groupId [{}] streamId [{}]",
                        groupId, streamId);
                return true;
            } else {
                log.info("skip to create pulsar topic as the createResource was false for groupId [{}] streamId [{}]",
                        groupId, streamId);
                return false;
            }
        }
        log.warn("no need to create pulsar topic for groupId={}, streamId={}, as the middlewareType={}",
                groupId, streamId, mqType);
        return false;

    }
}
