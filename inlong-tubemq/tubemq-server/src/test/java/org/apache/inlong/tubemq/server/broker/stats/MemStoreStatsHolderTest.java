/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.server.broker.stats;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.junit.Assert;
import org.junit.Test;

/**
 * MemStoreStatsHolder test.
 */
public class MemStoreStatsHolderTest {

    @Test
    public void testMemStoreStatsHolder() {
        MemStoreStatsHolder memStatsHolder = new MemStoreStatsHolder();
        // case 1, not started
        memStatsHolder.addAppendedMsgSize(50);
        memStatsHolder.addCacheFullType(true, false, false);
        memStatsHolder.addCacheFullType(false, true, false);
        memStatsHolder.addCacheFullType(false, false, true);
        memStatsHolder.addFlushTime(50, false);
        memStatsHolder.addFlushTime(10, true);
        memStatsHolder.addMsgWriteFail();
        Map<String, Long> retMap = new LinkedHashMap<>();
        memStatsHolder.getValue(retMap);
        Assert.assertNotNull(retMap.get("reset_time"));
        Assert.assertEquals(0, retMap.get("msg_in_count").longValue());
        Assert.assertEquals(Long.MIN_VALUE, retMap.get("msg_in_max").longValue());
        Assert.assertEquals(Long.MAX_VALUE, retMap.get("msg_in_min").longValue());
        Assert.assertEquals(0, retMap.get("msg_append_fail").longValue());
        Assert.assertEquals(0, retMap.get("data_size_full").longValue());
        Assert.assertEquals(0, retMap.get("index_size_full").longValue());
        Assert.assertEquals(0, retMap.get("msg_count_full").longValue());
        Assert.assertEquals(0, retMap.get("cache_time_full").longValue());
        Assert.assertEquals(0, retMap.get("flush_pending").longValue());
        Assert.assertEquals(0, retMap.get("cache_realloc").longValue());
        Assert.assertEquals(0, retMap.get("cache_flush_dlt_count").longValue());
        Assert.assertEquals(Long.MIN_VALUE, retMap.get("cache_flush_dlt_max").longValue());
        Assert.assertEquals(Long.MAX_VALUE, retMap.get("cache_flush_dlt_min").longValue());
        Assert.assertNotNull(retMap.get("read_time"));
        retMap.clear();
        // get content by StringBuilder
        StringBuilder strBuff = new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE);
        memStatsHolder.getValue(strBuff);
        // System.out.println(strBuff.toString());
        strBuff.delete(0, strBuff.length());
        memStatsHolder.getAllMemStatsInfo(true, strBuff);
        // System.out.println("getAllMemStatsInfo : " + strBuff);
        strBuff.delete(0, strBuff.length());
        // case 2 started
        memStatsHolder.addAppendedMsgSize(50);
        memStatsHolder.addAppendedMsgSize(500);
        memStatsHolder.addAppendedMsgSize(5);
        memStatsHolder.addCacheFullType(true, false, false);
        memStatsHolder.addCacheFullType(false, true, false);
        memStatsHolder.addCacheFullType(false, false, true);
        memStatsHolder.addFlushTime(50, false);
        memStatsHolder.addFlushTime(10, true);
        memStatsHolder.addFlushTime(100, true);
        memStatsHolder.addFlushTime(1, false);
        memStatsHolder.addMsgWriteFail();
        memStatsHolder.addMsgWriteFail();
        memStatsHolder.addCacheReAlloc();
        memStatsHolder.addCacheReAlloc();
        memStatsHolder.addCachePending();
        memStatsHolder.addCachePending();
        memStatsHolder.addCachePending();
        memStatsHolder.getValue(retMap);
        Assert.assertNotNull(retMap.get("reset_time"));
        Assert.assertEquals(3, retMap.get("msg_in_count").longValue());
        Assert.assertEquals(500, retMap.get("msg_in_max").longValue());
        Assert.assertEquals(5, retMap.get("msg_in_min").longValue());
        Assert.assertEquals(2, retMap.get("msg_append_fail").longValue());
        Assert.assertEquals(1, retMap.get("data_size_full").longValue());
        Assert.assertEquals(1, retMap.get("index_size_full").longValue());
        Assert.assertEquals(1, retMap.get("msg_count_full").longValue());
        Assert.assertEquals(2, retMap.get("cache_time_full").longValue());
        Assert.assertEquals(3, retMap.get("flush_pending").longValue());
        Assert.assertEquals(2, retMap.get("cache_realloc").longValue());
        Assert.assertEquals(4, retMap.get("cache_flush_dlt_count").longValue());
        Assert.assertEquals(100, retMap.get("cache_flush_dlt_max").longValue());
        Assert.assertEquals(1, retMap.get("cache_flush_dlt_min").longValue());
        Assert.assertEquals(1, retMap.get("cache_flush_dlt_cell_0t2").longValue());
        Assert.assertEquals(1, retMap.get("cache_flush_dlt_cell_8t16").longValue());
        Assert.assertEquals(1, retMap.get("cache_flush_dlt_cell_32t64").longValue());
        Assert.assertEquals(1, retMap.get("cache_flush_dlt_cell_64t128").longValue());
        Assert.assertNotNull(retMap.get("read_time"));
        memStatsHolder.getAllMemStatsInfo(false, strBuff);
        // System.out.println("\n the second is : " + strBuff.toString());
        strBuff.delete(0, strBuff.length());

    }
}
