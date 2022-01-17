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

package org.apache.inlong.store.service;

import org.apache.inlong.store.Application;
import org.apache.inlong.store.db.entities.ESDataPo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.testng.Assert;
import org.testng.annotations.AfterClass;

import java.io.IOException;
import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class ElasticsearchServiceTest {

    @Autowired
    private ElasticsearchService elasticsearchService;

    private String index1 = "20220112_1";
    private String index2 = "20220112_10";

    @Test
    public void testCreateIndex() throws IOException {
        boolean res = elasticsearchService.createIndex(index1);
        Assert.assertEquals(res, true);
    }

    @Test
    public void testExistsIndex() throws IOException {
        boolean res = elasticsearchService.createIndex(index1);
        Assert.assertEquals(res, true);

        res = elasticsearchService.existsIndex(index1);
        Assert.assertEquals(res, true);

        res = elasticsearchService.existsIndex(index2);
        Assert.assertEquals(res, false);
    }

    @Test
    public void testInsertData() {
        for (int i = 0; i < 10; i++) {
            ESDataPo po = new ESDataPo();
            po.setIp("0.0.0.0");
            po.setThreadId(String.valueOf(i));
            po.setDockerId(String.valueOf(i));
            po.setSdkTs(new Date().getTime());
            po.setLogTs(new Date());
            po.setAuditId("3");
            po.setCount(i);
            po.setDelay(i);
            po.setInlongGroupId(String.valueOf(i));
            po.setInlongStreamId(String.valueOf(i));
            po.setSize(i);
            elasticsearchService.insertData(po);
            ESDataPo po2=new ESDataPo();
            po2.setIp("0.0.0.0");
            po2.setThreadId(String.valueOf(i));
            po2.setDockerId(String.valueOf(i));
            po2.setSdkTs(new Date().getTime());
            po2.setLogTs(new Date());
            po2.setAuditId("2");
            po2.setCount(i);
            po2.setDelay(i);
            po2.setInlongGroupId(String.valueOf(i));
            po2.setInlongStreamId(String.valueOf(i));
            po2.setSize(i);
            elasticsearchService.insertData(po2);
        }
    }

    @Test
    public void testDeleteSingleIndex() throws IOException {
        boolean res = elasticsearchService.createIndex(index1);
        Assert.assertEquals(res, true);
        res = elasticsearchService.deleteSingleIndex(index1);
        Assert.assertEquals(res, true);
    }

    @Test
    public void testDeleteTimeoutIndexs() throws IOException {
        elasticsearchService.deleteTimeoutIndexs();
    }

    @AfterClass
    public void testClose() throws Exception {
        elasticsearchService.close();
    }

}
