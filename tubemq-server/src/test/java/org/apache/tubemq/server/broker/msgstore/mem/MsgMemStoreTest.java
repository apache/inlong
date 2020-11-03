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

package org.apache.tubemq.server.broker.msgstore.mem;

import java.nio.ByteBuffer;

import org.apache.tubemq.server.common.utils.AppendResult;
import org.junit.Test;

/***
 * MsgMemStore test.
 */
public class MsgMemStoreTest {

    @Test
    public void appendMsg() {
        int maxCacheSize = 2 * 1024 * 1024;
        int maxMsgCount = 10000;
        MsgMemStore msgMemStore = new MsgMemStore(maxCacheSize, maxMsgCount, null);
        MsgMemStatisInfo msgMemStatisInfo = new MsgMemStatisInfo();
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put("abc".getBytes());
        AppendResult appendResult = new AppendResult();
        // append data
        msgMemStore.appendMsg(msgMemStatisInfo, 0, 0,
                System.currentTimeMillis(), 3, bf, appendResult);
    }

    @Test
    public void getMessages() {
        int maxCacheSize = 2 * 1024 * 1024;
        int maxMsgCount = 10000;
        MsgMemStore msgMemStore = new MsgMemStore(maxCacheSize, maxMsgCount, null);
        MsgMemStatisInfo msgMemStatisInfo = new MsgMemStatisInfo();
        ByteBuffer bf = ByteBuffer.allocate(1024);
        bf.put("abc".getBytes());
        AppendResult appendResult = new AppendResult();
        msgMemStore.appendMsg(msgMemStatisInfo, 0, 0,
                System.currentTimeMillis(), 3, bf, appendResult);
        // get messages
        GetCacheMsgResult getCacheMsgResult = msgMemStore.getMessages(0, 2, 1024, 1000, 0, false, false, null);
    }
}
