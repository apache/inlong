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

package org.apache.inlong.manager.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.plugin.listener.SuspendSortListener;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SuspendSortListenerTest {

    @Test
    public void testListener() throws Exception {
        WorkflowContext context = new WorkflowContext();
        UpdateGroupProcessForm updateGroupProcessForm = new UpdateGroupProcessForm();
        context.setProcessForm(updateGroupProcessForm);
        InlongGroupInfo inlongGroupInfo = new InlongGroupInfo();
        inlongGroupInfo.setInlongGroupId("1");
        updateGroupProcessForm.setGroupInfo(inlongGroupInfo);

        Map<String, String> sortProperties = new HashMap<>(16);
        sortProperties.put(InlongGroupSettings.CLUSTER_ID, "cluster-3pcta51b");

        InlongGroupExtInfo inlongGroupExtInfo1 = new InlongGroupExtInfo();
        inlongGroupExtInfo1.setKeyName(InlongGroupSettings.SORT_URL);
        inlongGroupExtInfo1.setKeyValue("9.135.80.112:8085");
        List<InlongGroupExtInfo> inlongGroupExtInfos = new ArrayList<>();
        inlongGroupExtInfos.add(inlongGroupExtInfo1);

        InlongGroupExtInfo inlongGroupExtInfo2 = new InlongGroupExtInfo();
        inlongGroupExtInfo2.setKeyName(InlongGroupSettings.SORT_PROPERTIES);
        ObjectMapper objectMapper = new ObjectMapper();
        String sortStr = objectMapper.writeValueAsString(sortProperties);
        inlongGroupExtInfo2.setKeyValue(sortStr);
        inlongGroupExtInfos.add(inlongGroupExtInfo2);

        InlongGroupExtInfo inlongGroupExtInfo5 = new InlongGroupExtInfo();
        inlongGroupExtInfo5.setKeyName(InlongGroupSettings.SORT_JOB_ID);
        inlongGroupExtInfo5.setKeyValue("8f87b7975d03625531bc87a70475a198");
        inlongGroupExtInfos.add(inlongGroupExtInfo5);

        inlongGroupInfo.setExtList(inlongGroupExtInfos);

        SuspendSortListener pauseSortListener = new SuspendSortListener();
        pauseSortListener.listen(context);
        while (true) {
        }
    }
}
