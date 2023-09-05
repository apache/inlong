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

package org.apache.inlong.tubemq.server.tools.cli;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class MessageCommandTest {

    CommandToolMain tubectlTool = null;

    @Before
    public void setUp() {
        tubectlTool = new CommandToolMain();
    }

    @Test
    public void testMessageProduceSync() throws UnknownHostException, InterruptedException {
        InetAddress addr = InetAddress.getLocalHost();
        int port = 8715;
        String masterservers = addr.getHostAddress() + ":" + String.valueOf(port);

        String messageBody = "This is a message from testMessageProduceSync.";
        InputStream in = new ByteArrayInputStream(messageBody.getBytes());
        System.setIn(in);

        String[] arg = {"message", "produce", "-ms", masterservers, "-t", "b4t4", "-m", "sync", "-mt", "1"};
        Assert.assertTrue(tubectlTool.run(arg));

    }

    @Test
    public void testMessageProduceAsync() throws UnknownHostException, InterruptedException {
        InetAddress addr = InetAddress.getLocalHost();
        int port = 8715;
        String masterservers = addr.getHostAddress() + ":" + String.valueOf(port);

        String messageBody = "This is a message from testMessageProduceAsync.";
        InputStream in = new ByteArrayInputStream(messageBody.getBytes());
        System.setIn(in);

        String[] arg = {"message", "produce", "-ms", masterservers, "-t", "b4t4", "-m", "async", "-mt", "1"};
        Assert.assertTrue(tubectlTool.run(arg));

    }

}
