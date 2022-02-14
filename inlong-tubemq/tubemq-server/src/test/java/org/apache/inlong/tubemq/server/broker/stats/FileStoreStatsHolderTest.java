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
 * FileStoreStatsHolder test.
 */
public class FileStoreStatsHolderTest {

    @Test
    public void testFileStoreStatsHolder() {
        FileStoreStatsHolder fileStatsHolder = new FileStoreStatsHolder();
        // case 1, not started
        fileStatsHolder.addFileFlushStatsInfo(2, 30, 500,
                0, 0, true, true,
                true, true, true, true);
        fileStatsHolder.addTimeoutFlush(1, 500, false);
        Map<String, Long> retMap = new LinkedHashMap<>();
        fileStatsHolder.getValue(retMap);
        Assert.assertNotNull(retMap.get("reset_time"));
        Assert.assertEquals(0, retMap.get("total_msg_cnt").longValue());
        Assert.assertEquals(0, retMap.get("total_data_size").longValue());
        Assert.assertEquals(0, retMap.get("total_index_size").longValue());
        Assert.assertEquals(0, retMap.get("flushed_data_size_count").longValue());
        Assert.assertEquals(Long.MIN_VALUE, retMap.get("flushed_data_size_max").longValue());
        Assert.assertEquals(Long.MAX_VALUE, retMap.get("flushed_data_size_min").longValue());
        Assert.assertEquals(0, retMap.get("flushed_msg_cnt_count").longValue());
        Assert.assertEquals(Long.MIN_VALUE, retMap.get("flushed_msg_cnt_max").longValue());
        Assert.assertEquals(Long.MAX_VALUE, retMap.get("flushed_msg_cnt_min").longValue());
        Assert.assertEquals(0, retMap.get("data_seg_cnt").longValue());
        Assert.assertEquals(0, retMap.get("index_seg_cnt").longValue());
        Assert.assertEquals(0, retMap.get("data_size_full").longValue());
        Assert.assertEquals(0, retMap.get("meta_flush_cnt").longValue());
        Assert.assertEquals(0, retMap.get("msg_count_full").longValue());
        Assert.assertEquals(0, retMap.get("cache_time_full").longValue());
        Assert.assertNotNull(retMap.get("end_time"));
        retMap.clear();
        // get content by StringBuilder
        StringBuilder strBuff = new StringBuilder(TBaseConstants.BUILDER_DEFAULT_SIZE);
        fileStatsHolder.getValue(strBuff);
        System.out.println(strBuff.toString());
        strBuff.delete(0, strBuff.length());
        // test timeout
        fileStatsHolder.addTimeoutFlush(1, 1, true);
        fileStatsHolder.getValue(retMap);
        Assert.assertNotNull(retMap.get("reset_time"));
        Assert.assertEquals(0, retMap.get("total_msg_cnt").longValue());
        Assert.assertEquals(0, retMap.get("total_data_size").longValue());
        Assert.assertEquals(0, retMap.get("total_index_size").longValue());
        Assert.assertEquals(1, retMap.get("flushed_data_size_count").longValue());
        Assert.assertEquals(1, retMap.get("flushed_data_size_max").longValue());
        Assert.assertEquals(1, retMap.get("flushed_data_size_min").longValue());
        Assert.assertEquals(1, retMap.get("flushed_msg_cnt_count").longValue());
        Assert.assertEquals(1, retMap.get("flushed_msg_cnt_max").longValue());
        Assert.assertEquals(1, retMap.get("flushed_msg_cnt_min").longValue());
        Assert.assertEquals(0, retMap.get("data_seg_cnt").longValue());
        Assert.assertEquals(0, retMap.get("index_seg_cnt").longValue());
        Assert.assertEquals(1, retMap.get("meta_flush_cnt").longValue());
        Assert.assertEquals(0, retMap.get("data_size_full").longValue());
        Assert.assertEquals(0, retMap.get("msg_count_full").longValue());
        Assert.assertEquals(1, retMap.get("cache_time_full").longValue());
        Assert.assertNotNull(retMap.get("end_time"));
        retMap.clear();
        // get value when started
        fileStatsHolder.addFileFlushStatsInfo(1, 1, 1,
                1, 1, true, false,
                false, false, false, false);
        fileStatsHolder.addFileFlushStatsInfo(6, 6, 6,
                6, 6, false, false,
                false, false, false, true);
        fileStatsHolder.addFileFlushStatsInfo(2, 2, 2,
                2, 2, false, true,
                false, false, false, false);
        fileStatsHolder.addFileFlushStatsInfo(5, 5, 5,
                5, 5, false, false,
                false, false, true, false);
        fileStatsHolder.addFileFlushStatsInfo(4, 4, 4,
                4, 4, false, false,
                false, true, false, false);
        fileStatsHolder.addFileFlushStatsInfo(3, 3, 3,
                3, 3, false, false,
                true, false, false, false);
        fileStatsHolder.snapShort(retMap);
        Assert.assertNotNull(retMap.get("reset_time"));
        Assert.assertEquals(21, retMap.get("total_msg_cnt").longValue());
        Assert.assertEquals(21, retMap.get("total_data_size").longValue());
        Assert.assertEquals(21, retMap.get("total_index_size").longValue());
        Assert.assertEquals(7, retMap.get("flushed_data_size_count").longValue());
        Assert.assertEquals(6, retMap.get("flushed_data_size_max").longValue());
        Assert.assertEquals(1, retMap.get("flushed_data_size_min").longValue());
        Assert.assertEquals(7, retMap.get("flushed_msg_cnt_count").longValue());
        Assert.assertEquals(6, retMap.get("flushed_msg_cnt_max").longValue());
        Assert.assertEquals(1, retMap.get("flushed_msg_cnt_min").longValue());
        Assert.assertEquals(1, retMap.get("data_seg_cnt").longValue());
        Assert.assertEquals(1, retMap.get("index_seg_cnt").longValue());
        Assert.assertEquals(1, retMap.get("data_size_full").longValue());
        Assert.assertEquals(2, retMap.get("meta_flush_cnt").longValue());
        Assert.assertEquals(1, retMap.get("msg_count_full").longValue());
        Assert.assertEquals(2, retMap.get("cache_time_full").longValue());
        Assert.assertNotNull(retMap.get("end_time"));
        retMap.clear();
        fileStatsHolder.getAllFileStatsInfo(true, strBuff);
        System.out.println(strBuff.toString());
        strBuff.delete(0, strBuff.length());

    }
}
