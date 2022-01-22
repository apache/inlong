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

package org.apache.inlong.manager.service.core;

import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfo;
import org.apache.inlong.manager.web.ServiceBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;

/**
 * Data stream service test
 */
@TestComponent
public class DataStreamServiceTest extends ServiceBaseTest {

    private final String globalGroupId = "b_group1";
    private final String globalGroupName = "group1";
    private final String globalStreamId = "stream1";
    private final String globalOperator = "test_user";

    @Autowired
    private DataStreamService dataStreamService;
    @Autowired
    private BusinessServiceTest businessServiceTest;

    public Integer saveDataStream(String groupId, String streamId, String operator) {
        DataStreamInfo streamInfo;
        try {
            streamInfo = dataStreamService.get(groupId, streamId);
            if (streamInfo != null) {
                return streamInfo.getId();
            }
        } catch (Exception e) {
            // ignore
        }

        businessServiceTest.saveBusiness(globalGroupName, operator);

        streamInfo = new DataStreamInfo();
        streamInfo.setInlongGroupId(groupId);
        streamInfo.setInlongStreamId(streamId);
        streamInfo.setDataEncoding("UTF-8");

        return dataStreamService.save(streamInfo, operator);
    }

    @Test
    public void testSaveAndDelete() {
        Integer id = this.saveDataStream(globalGroupId, globalStreamId, globalOperator);
        Assert.assertNotNull(id);

        boolean result = dataStreamService.delete(globalGroupId, globalStreamId, globalOperator);
        Assert.assertTrue(result);
    }

}
