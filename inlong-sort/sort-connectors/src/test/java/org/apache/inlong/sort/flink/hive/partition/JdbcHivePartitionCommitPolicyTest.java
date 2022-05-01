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

package org.apache.inlong.sort.flink.hive.partition;

import org.junit.Assert;
import org.junit.Test;

public class JdbcHivePartitionCommitPolicyTest {

    @Test
    public void testGetHiveConnStr1() {
        String hiveJdbcUrl = "jdbc:hive2://127.0.0.1:10000";
        String database = "test666";
        String expectValue = "jdbc:hive2://127.0.0.1:10000/test666";
        Assert.assertEquals(JdbcHivePartitionCommitPolicy.getHiveConnStr(hiveJdbcUrl, database), expectValue);
    }

    @Test
    public void testGetHiveConnStr2() {
        String hiveJdbcUrl = "jdbc:hive2://127.0.0.1:10000/";
        String database = "test666";
        String expectValue = "jdbc:hive2://127.0.0.1:10000/test666";
        Assert.assertEquals(JdbcHivePartitionCommitPolicy.getHiveConnStr(hiveJdbcUrl, database), expectValue);
    }

    @Test
    public void testGetHiveConnStr3() {
        String hiveJdbcUrl = "jdbc:hive2://127.0.0.1:10000/test888";
        String database = "test666";
        String expectValue = "jdbc:hive2://127.0.0.1:10000/test666";
        Assert.assertEquals(JdbcHivePartitionCommitPolicy.getHiveConnStr(hiveJdbcUrl, database), expectValue);
    }

    @Test
    public void testGetHiveConnStr4() {
        String hiveJdbcUrl = "jdbc:hive2://127.0.0.1:10000/test888/;principal=hive/127.0.0.1@TEST.COM";
        String database = "test666";
        String expectValue = "jdbc:hive2://127.0.0.1:10000/test666/;principal=hive/127.0.0.1@TEST.COM";
        Assert.assertEquals(JdbcHivePartitionCommitPolicy.getHiveConnStr(hiveJdbcUrl, database), expectValue);
    }

    @Test
    public void testGetHiveConnStr5() {
        String hiveJdbcUrl = "jdbc:hive2://127.0.0.1:10000/test888/test888;principal=hive/127.0.0.1@TEST.COM";
        String database = "test666";
        String expectValue = "jdbc:hive2://127.0.0.1:10000/test666/;principal=hive/127.0.0.1@TEST.COM";
        Assert.assertEquals(JdbcHivePartitionCommitPolicy.getHiveConnStr(hiveJdbcUrl, database), expectValue);
    }

}
