/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.master.metamanage.metastore.dao.entity;

import java.util.Date;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbGroupFilterCondEntity;
import org.junit.Assert;
import org.junit.Test;




public class GroupConsumeCtrlEntityTest {

    @Test
    public void groupConsumeCtrlEntityTest() {
        // case 1
        String topicName = "test_1";
        String groupName = "group_1";
        int controlStatus = 2;
        String filterCondStr = "[1,2,3,4]";
        String attributes = "key=val&ke2=val3";
        String createUser = "creater";
        Date createDate = new Date();
        BdbGroupFilterCondEntity bdbEntity1 =
                new BdbGroupFilterCondEntity(topicName, groupName, controlStatus,
                        filterCondStr, attributes, createUser, createDate);

        GroupConsumeCtrlEntity ctrlEntry1 = new GroupConsumeCtrlEntity(bdbEntity1);
        Assert.assertEquals(ctrlEntry1.getGroupName(), bdbEntity1.getConsumerGroupName());
        Assert.assertEquals(ctrlEntry1.getTopicName(), bdbEntity1.getTopicName());
        Assert.assertEquals(ctrlEntry1.getFilterCondStr(), bdbEntity1.getFilterCondStr());
        Assert.assertEquals(bdbEntity1.getControlStatus(), 2);
        Assert.assertTrue(ctrlEntry1.getFilterEnable().isEnable());
        Assert.assertEquals(ctrlEntry1.getConsumeEnable(), bdbEntity1.getConsumeEnable());
        Assert.assertEquals(ctrlEntry1.getCreateUser(), bdbEntity1.getCreateUser());
        Assert.assertEquals(ctrlEntry1.getCreateDate(), bdbEntity1.getCreateDate());
        Assert.assertEquals(ctrlEntry1.getAttributes(), bdbEntity1.getAttributes());
        Assert.assertEquals(ctrlEntry1.getDisableReason(), bdbEntity1.getDisableConsumeReason());
        Assert.assertEquals(ctrlEntry1.getRecordKey(), bdbEntity1.getRecordKey());
        // case 2
        long newDataVerId = 5555;
        boolean consumeEnable = true;
        String disableRsn = "disable";
        boolean filterEnable = true;
        String newFilterCondStr = "[1,2,4]";
        BaseEntity opInfoEntity =
                new BaseEntity(newDataVerId, "modify", new Date());
        GroupConsumeCtrlEntity ctrlEntry2 = ctrlEntry1.clone();
        Assert.assertTrue(ctrlEntry2.isMatched(ctrlEntry1));
        ctrlEntry2.updBaseModifyInfo(opInfoEntity);
        Assert.assertTrue(ctrlEntry2.updModifyInfo(opInfoEntity.getDataVerId(),
                consumeEnable, disableRsn, filterEnable, newFilterCondStr));
        // case 3
        BdbGroupFilterCondEntity bdbEntity3 = ctrlEntry2.buildBdbGroupFilterCondEntity();
        Assert.assertEquals(ctrlEntry2.getGroupName(), bdbEntity3.getConsumerGroupName());
        Assert.assertEquals(ctrlEntry2.getTopicName(), bdbEntity3.getTopicName());
        Assert.assertEquals(ctrlEntry2.getFilterCondStr(), bdbEntity3.getFilterCondStr());
        Assert.assertEquals(bdbEntity3.getControlStatus(), 2);
        Assert.assertTrue(ctrlEntry2.getFilterEnable().isEnable());
        Assert.assertEquals(ctrlEntry2.getConsumeEnable(), bdbEntity3.getConsumeEnable());
        Assert.assertEquals(opInfoEntity.getCreateUser(), bdbEntity3.getCreateUser());
        Assert.assertEquals(opInfoEntity.getCreateDate(), bdbEntity3.getCreateDate());
        Assert.assertNotEquals(ctrlEntry2.getCreateDate(), bdbEntity3.getCreateDate());
        Assert.assertNotEquals(ctrlEntry2.getCreateUser(), bdbEntity3.getCreateUser());
        Assert.assertEquals(ctrlEntry2.getAttributes(), bdbEntity3.getAttributes());
        Assert.assertEquals(ctrlEntry2.getDisableReason(), bdbEntity3.getDisableConsumeReason());
        Assert.assertEquals(ctrlEntry2.getRecordKey(), bdbEntity3.getRecordKey());




    }

}
