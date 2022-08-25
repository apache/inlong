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

package org.apache.inlong.dataproxy.channel;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

/**
 * BufferQueueChannelTest
 * 
 */
public class TestBufferQueueChannel {

    /**
     * testResult
     * MemoryChannel,times:1000000,transactionSize:1,duration:1363<br>
     * BufferQueueChannel,times:1000000,transactionSize:1,duration:84,gap:0.9383712399119589<br>
     * MemoryChannel,times:1000000,transactionSize:10,duration:2818<br>
     * BufferQueueChannel,times:1000000,transactionSize:10,duration:122,gap:0.9567068843151171<br>
     * MemoryChannel,times:1000000,transactionSize:100,duration:19189<br>
     * BufferQueueChannel,times:1000000,transactionSize:100,duration:687,gap:0.9641982385741831<br>
     * @throws Exception
     */
    @Test
    public void testResult() throws Exception {
        // init
        Context memoryContext = new Context();
        memoryContext.put("capacity", "50000");
        memoryContext.put("keep-alive", "0");
        memoryContext.put("transactionCapacity", "200");
        final MemoryChannel memoryChannel = new MemoryChannel();
        memoryChannel.configure(memoryContext);
        memoryChannel.start();
        // init
        Context bufferContext = new Context();
        bufferContext.put("maxBufferQueueCount", "50000");
        bufferContext.put("maxBufferQueueSizeKb", "50000");
        final BufferQueueChannel bufferChannel = new BufferQueueChannel();
        bufferChannel.configure(bufferContext);
        bufferChannel.start();
        // times
        int times = 1000000;
        int transactionSize = 1;
        long memoryDuration = 0;
        long bufferDuration = 0;
        // transactionSize=1
        transactionSize = 1;
        memoryDuration = runChannel(memoryChannel, times, transactionSize);
        System.out.println(String.format("MemoryChannel,times:%d,transactionSize:%d,duration:%d", times,
                transactionSize, memoryDuration));
        bufferDuration = runChannel(bufferChannel, times, transactionSize);
        System.out.println(String.format("BufferQueueChannel,times:%d,transactionSize:%d,duration:%d,gap:%s", times,
                transactionSize, bufferDuration,
                String.valueOf((memoryDuration - bufferDuration) * 1.0 / memoryDuration)));
        // transactionSize=10
        transactionSize = 10;
        memoryDuration = runChannel(memoryChannel, times, transactionSize);
        System.out.println(String.format("MemoryChannel,times:%d,transactionSize:%d,duration:%d", times,
                transactionSize, memoryDuration));
        bufferDuration = runChannel(bufferChannel, times, transactionSize);
        System.out.println(String.format("BufferQueueChannel,times:%d,transactionSize:%d,duration:%d,gap:%s", times,
                transactionSize, bufferDuration,
                String.valueOf((memoryDuration - bufferDuration) * 1.0 / memoryDuration)));
        // transactionSize=100
        transactionSize = 100;
        memoryDuration = runChannel(memoryChannel, times, transactionSize);
        System.out.println(String.format("MemoryChannel,times:%d,transactionSize:%d,duration:%d", times,
                transactionSize, memoryDuration));
        bufferDuration = runChannel(bufferChannel, times, transactionSize);
        System.out.println(String.format("BufferQueueChannel,times:%d,transactionSize:%d,duration:%d,gap:%s", times,
                transactionSize, bufferDuration,
                String.valueOf((memoryDuration - bufferDuration) * 1.0 / memoryDuration)));
    }

    /**
     * runChannel
     * @param channel
     * @param times
     * @param transactionSize
     * @return
     */
    private long runChannel(Channel channel, int times, int transactionSize) {
        long startTime = System.currentTimeMillis();
        try {
            SimpleEvent event = new SimpleEvent();
            for (int i = 0; i < times; i++) {
                // put
                Transaction putTx = channel.getTransaction();
                putTx.begin();
                for (int j = 0; j < transactionSize; j++) {
                    channel.put(event);
                }
                putTx.commit();
                putTx.close();
                // take
                Transaction takeTx = channel.getTransaction();
                takeTx.begin();
                for (int j = 0; j < transactionSize; j++) {
                    channel.take();
                }
                takeTx.commit();
                takeTx.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }
}
