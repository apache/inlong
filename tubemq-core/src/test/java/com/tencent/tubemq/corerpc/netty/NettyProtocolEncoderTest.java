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

package com.tencent.tubemq.corerpc.netty;

import com.tencent.tubemq.corerpc.RpcDataPack;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Assert;
import org.junit.Test;

/***
 * NettyProtocolEncoder test.
 */
public class NettyProtocolEncoderTest {

    @Test
    public void encode() {
        NettyProtocolEncoder nettyProtocolEncoder = new NettyProtocolEncoder();
        // build RpcDataPack
        RpcDataPack obj = new RpcDataPack();
        // set serial number
        obj.setSerialNo(123);
        List<ByteBuffer> dataList = new LinkedList<ByteBuffer>();
        dataList.add(ByteBuffer.wrap("abc".getBytes()));
        dataList.add(ByteBuffer.wrap("def".getBytes()));
        // append data list.
        obj.setDataLst(dataList);
        try {
            // encode data
            Object result = nettyProtocolEncoder.encode(null, null, obj);
            ChannelBuffer buf = (ChannelBuffer) result;
            // read data.
            int i = buf.readInt();
            i = buf.readInt();
            Assert.assertTrue(i == 123);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
