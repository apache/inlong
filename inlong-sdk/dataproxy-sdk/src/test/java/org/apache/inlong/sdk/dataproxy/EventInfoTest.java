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

package org.apache.inlong.sdk.dataproxy;

import org.apache.inlong.sdk.dataproxy.exception.ProxyEventException;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpEventInfo;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpEventInfo;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventInfoTest {

    @Test
    public void testTcpEventInfo() throws Exception {
        // case a1 normal data setting
        String groupId = "groupId";
        String streamId = "streamId";
        long dtMs = System.currentTimeMillis();
        Map<String, String> attrsA1 = null;
        byte[] body = "test".getBytes(StandardCharsets.UTF_8);
        TcpEventInfo eventInfoA1 =
                new TcpEventInfo(groupId, streamId, dtMs, attrsA1, body);
        Assert.assertEquals(groupId, eventInfoA1.getGroupId());
        Assert.assertEquals(streamId, eventInfoA1.getStreamId());
        Assert.assertEquals(dtMs, eventInfoA1.getDtMs());
        Assert.assertNotNull(eventInfoA1.getAttrs());
        Assert.assertEquals(0, eventInfoA1.getAttrs().size());
        Assert.assertEquals(1, eventInfoA1.getMsgCnt());
        Assert.assertEquals(body.length, eventInfoA1.getBodySize());
        Assert.assertArrayEquals(body, eventInfoA1.getBodyList().get(0));
        eventInfoA1.setAttr("a1key", "mmm");
        Assert.assertEquals(1, eventInfoA1.getAttrs().size());
        Assert.assertEquals(1, eventInfoA1.getMsgCnt());
        // case a2 normal data setting
        Map<String, String> attrsA2 = new HashMap<>();
        List<byte[]> bodyListA2 = new ArrayList<>();
        bodyListA2.add("test_msg_1".getBytes(StandardCharsets.UTF_8));
        bodyListA2.add("test_msg_2".getBytes(StandardCharsets.UTF_8));
        TcpEventInfo eventInfoA2 =
                new TcpEventInfo(groupId, streamId, dtMs, attrsA2, bodyListA2);
        Assert.assertEquals(groupId, eventInfoA2.getGroupId());
        Assert.assertEquals(streamId, eventInfoA2.getStreamId());
        Assert.assertEquals(dtMs, eventInfoA2.getDtMs());
        Assert.assertEquals(attrsA2, eventInfoA2.getAttrs());
        Assert.assertEquals(2, eventInfoA2.getMsgCnt());
        int totalSize = 0;
        List<byte[]> tgtListA2 = eventInfoA2.getBodyList();
        for (byte[] b : bodyListA2) {
            Assert.assertTrue(tgtListA2.contains(b));
            totalSize += b.length;
        }
        Assert.assertEquals(totalSize, eventInfoA2.getBodySize());
        eventInfoA2.setAttr("a2key", "ccc");
        Assert.assertEquals(1, eventInfoA2.getAttrs().size());
        // case A3 abnormal data setting
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(null, streamId, dtMs, attrsA1, body));
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo("   ", streamId, dtMs, attrsA1, body));
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, null, dtMs, attrsA1, body));
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, "  ", dtMs, attrsA1, body));
        byte[] bodyA301 = null;
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA1, bodyA301));
        byte[] bodyA302 = "".getBytes(StandardCharsets.UTF_8);
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA1, bodyA302));
        byte[] bodyA303 = "    ".getBytes(StandardCharsets.UTF_8);
        TcpEventInfo eventInfoA303 =
                new TcpEventInfo(groupId, streamId, dtMs, attrsA1, bodyA303);
        Assert.assertEquals(1, eventInfoA303.getMsgCnt());
        Assert.assertEquals(bodyA303.length, eventInfoA303.getBodySize());
        List<byte[]> msgListA311 = null;
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA1, msgListA311));
        List<byte[]> msgListA312 = new ArrayList<>();
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA1, msgListA312));
        List<byte[]> msgListA313 = new ArrayList<>();
        msgListA313.add(null);
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA1, msgListA313));
        List<byte[]> msgListA314 = new ArrayList<>();
        msgListA314.add("".getBytes(StandardCharsets.UTF_8));
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA1, msgListA314));
        List<byte[]> msgListA315 = new ArrayList<>();
        msgListA315.add("".getBytes(StandardCharsets.UTF_8));
        msgListA315.add("test".getBytes(StandardCharsets.UTF_8));
        TcpEventInfo eventInfoA315 =
                new TcpEventInfo(groupId, streamId, dtMs, attrsA1, msgListA315);
        Assert.assertEquals(1, eventInfoA315.getMsgCnt());
        Assert.assertEquals("test".getBytes(StandardCharsets.UTF_8).length, eventInfoA315.getBodySize());
        // case A4 normal attributes setting
        Map<String, String> attrsA41 = new HashMap<>();
        attrsA41.put("aaa&mmm", "value");
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA41, body));
        Map<String, String> attrsA42 = new HashMap<>();
        attrsA42.put("aaa=mmm", "value");
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA42, body));
        Map<String, String> attrsA43 = new HashMap<>();
        attrsA43.put("groupId", "value");
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA43, body));
        Map<String, String> attrsA44 = new HashMap<>();
        attrsA44.put("testA44", "va&lue");
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA44, body));
        Map<String, String> attrsA45 = new HashMap<>();
        attrsA45.put("testA45", "va=lue");
        Assert.assertThrows(ProxyEventException.class,
                () -> new TcpEventInfo(groupId, streamId, dtMs, attrsA45, body));
        TcpEventInfo eventInfoA46 =
                new TcpEventInfo(groupId, streamId, dtMs, attrsA1, body);
        Assert.assertThrows(ProxyEventException.class,
                () -> eventInfoA46.setAttr("aaa&mmm", "value"));
        Assert.assertThrows(ProxyEventException.class,
                () -> eventInfoA46.setAttr("streamId", "value"));
        Assert.assertThrows(ProxyEventException.class,
                () -> eventInfoA46.setAttr("kkk=mmm", "value"));
        Assert.assertThrows(ProxyEventException.class,
                () -> eventInfoA46.setAttr("aaaa", "va=lue"));
        Assert.assertThrows(ProxyEventException.class,
                () -> eventInfoA46.setAttr("aaaa", "va&lue"));
        // case 5, set uuid, auditVersion
        String uuid = "uuid";
        long auditVer = 32L;
        TcpEventInfo eventInfoA51 =
                new TcpEventInfo(groupId, streamId, dtMs, auditVer, attrsA1, body);
        Assert.assertNotNull(eventInfoA51.getAttrs());
        Assert.assertEquals(1, eventInfoA51.getAttrs().size());
        TcpEventInfo eventInfoA52 =
                new TcpEventInfo(groupId, streamId, dtMs, uuid, attrsA1, body);
        Assert.assertNotNull(eventInfoA52.getAttrs());
        Assert.assertEquals(1, eventInfoA52.getAttrs().size());
        TcpEventInfo eventInfoA53 =
                new TcpEventInfo(groupId, streamId, dtMs, auditVer, uuid, attrsA1, body);
        Assert.assertNotNull(eventInfoA53.getAttrs());
        Assert.assertEquals(2, eventInfoA53.getAttrs().size());
    }

    @Test
    public void testHttpEventInfo() throws Exception {
        // case a1 normal data setting
        String groupId = "groupId";
        String streamId = "streamId";
        long dtMs = System.currentTimeMillis();
        String body = "test";
        HttpEventInfo eventInfoA1 =
                new HttpEventInfo(groupId, streamId, dtMs, body);
        Assert.assertEquals(groupId, eventInfoA1.getGroupId());
        Assert.assertEquals(streamId, eventInfoA1.getStreamId());
        Assert.assertEquals(dtMs, eventInfoA1.getDtMs());
        Assert.assertNotNull(eventInfoA1.getAttrs());
        Assert.assertTrue(eventInfoA1.getAttrs().isEmpty());
        Assert.assertEquals(1, eventInfoA1.getMsgCnt());
        Assert.assertEquals(body.length(), eventInfoA1.getBodySize());
        Assert.assertEquals(body, eventInfoA1.getBodyList().get(0));
        // case A2 normal setting
        List<String> bodyListA2 = new ArrayList<>();
        bodyListA2.add("test_body_1");
        bodyListA2.add("test_body_2");
        HttpEventInfo eventInfoA2 =
                new HttpEventInfo(groupId, streamId, dtMs, bodyListA2);
        Assert.assertEquals(groupId, eventInfoA2.getGroupId());
        Assert.assertEquals(streamId, eventInfoA2.getStreamId());
        Assert.assertEquals(dtMs, eventInfoA2.getDtMs());
        Assert.assertNotNull(eventInfoA2.getAttrs());
        Assert.assertTrue(eventInfoA2.getAttrs().isEmpty());
        Assert.assertEquals(2, eventInfoA2.getMsgCnt());
        int totalSize = 0;
        List<String> tgtA2 = eventInfoA2.getBodyList();
        for (String item : tgtA2) {
            Assert.assertTrue(bodyListA2.contains(item));
            totalSize += item.length();
        }
        Assert.assertEquals(totalSize, eventInfoA2.getBodySize());
        // case A3 auditVer
        long auditVer = 1000L;
        HttpEventInfo eventInfoA3 =
                new HttpEventInfo(groupId, streamId, dtMs, auditVer, body);
        Assert.assertNotNull(eventInfoA3.getAttrs());
        Assert.assertEquals(1, eventInfoA3.getAttrs().size());
        // case A4 abnormal setting
        Assert.assertThrows(ProxyEventException.class,
                () -> new HttpEventInfo(null, streamId, dtMs, body));
        Assert.assertThrows(ProxyEventException.class,
                () -> new HttpEventInfo("   ", streamId, dtMs, body));
        Assert.assertThrows(ProxyEventException.class,
                () -> new HttpEventInfo(groupId, null, dtMs, body));
        Assert.assertThrows(ProxyEventException.class,
                () -> new HttpEventInfo(groupId, "  ", dtMs, body));
        String bodyA401 = null;
        Assert.assertThrows(ProxyEventException.class,
                () -> new HttpEventInfo(groupId, streamId, dtMs, bodyA401));
        String bodyA402 = "";
        Assert.assertThrows(ProxyEventException.class,
                () -> new HttpEventInfo(groupId, streamId, dtMs, bodyA402));
        String bodyA403 = "      ";
        Assert.assertThrows(ProxyEventException.class,
                () -> new HttpEventInfo(groupId, streamId, dtMs, bodyA403));
    }
}
