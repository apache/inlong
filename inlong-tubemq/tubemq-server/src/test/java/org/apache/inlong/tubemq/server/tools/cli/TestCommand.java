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

public class TestCommand {

    CommandToolMain tubectlTool = null;

    @Before
    public void setUp() {
        tubectlTool = new CommandToolMain();
    }

    @Test
    public void testTest() {
        Assert.assertTrue(true);
    }

    @Test
    public void testHelp() {
        String[] arg1 = {"-h"};
        Assert.assertTrue(tubectlTool.run(arg1));
    }

    @Test
    public void testTopicCreate() {
        String[] arg = {"topic", "create", "-n", "b4t1", "-bid", "4", "-c", "admin", "-at", "abc"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testTopicList() {
        String[] arg = {"topic", "list"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testTopicUpdate() {
        String[] arg = {"topic", "update", "-n", "b4t1", "-bid", "4", "-m", "admin", "-at", "abc"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testTopicDeleteSoft() {
        String[] arg = {"topic", "delete", "-o", "soft", "-n", "b4t1", "-bid", "4", "-m", "admin", "-at", "abc"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testTopicDeleteRedo() {
        String[] arg = {"topic", "delete", "-o", "redo", "-n", "b4t1", "-bid", "4", "-m", "admin", "-at", "abc"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testTopicDeleteHard() {
        String[] arg = {"topic", "delete", "-o", "hard", "-n", "b4t1", "-bid", "4", "-m", "admin", "-at", "abc"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testCgroupCreate() {
        String[] arg = {"cgroup", "create", "-n", "b4t1", "-g", "b4t1g1", "-at", "abc", "-c", "admin", "-cd",
                "20151117151129"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testCgroupList() {
        String[] arg = {"cgroup", "list", "-n", "b4t1", "-g", "b4t1g1", "-c", "admin"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testCgroupDelete() {
        String[] arg = {"cgroup", "delete", "-n", "b4t1", "-at", "abc", "-m", "admin", "-g", "b4t1g1"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testMessageProduceSync() throws UnknownHostException, InterruptedException {
        InetAddress addr = InetAddress.getLocalHost();
        int port = 8715;
        String masterservers = addr.getHostAddress() + ":" + String.valueOf(port);

        String messageBody = "This is a message from testMessageProduceSync.";
        InputStream in = new ByteArrayInputStream(messageBody.getBytes());
        System.setIn(in);

        String[] arg = {"message", "produce", "-ms", masterservers, "-n", "b4t4", "-m", "sync", "-t", "1"};
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

        String[] arg = {"message", "produce", "-ms", masterservers, "-n", "b4t4", "-m", "async", "-t", "1"};
        Assert.assertTrue(tubectlTool.run(arg));

    }

    // @Test
    // public void testMessageConsumePull() throws UnknownHostException {
    // InetAddress addr = InetAddress.getLocalHost();
    // int port = 8715;
    // String masterservers = addr.getHostAddress() + ":" + String.valueOf(port);
    //
    // String[] arg = {"message", "consume", "-ms", masterservers, "-n", "b4t4", "-g", "b4t4g4", "-m", "pull"};
    // Assert.assertTrue(tubectlTool.run(arg));
    //
    // }
    //
    // @Test
    // public void testMessageConsumePush() throws UnknownHostException {
    // InetAddress addr = InetAddress.getLocalHost();
    // int port = 8715;
    // String masterservers = addr.getHostAddress() + ":" + String.valueOf(port);
    //
    // String[] arg = {"message", "consume", "-ms", masterservers, "-n", "b4t4", "-g", "b4t4g4", "-m", "push"};
    // Assert.assertTrue(tubectlTool.run(arg));
    // }
    //
    // @Test
    // public void testMessageConsumeBalance() throws UnknownHostException {
    // InetAddress addr = InetAddress.getLocalHost();
    // int port = 8715;
    // String masterservers = addr.getHostAddress() + ":" + String.valueOf(port);
    //
    // String[] arg = {"message", "consume", "-ms", masterservers, "-n", "b4t1", "-g", "b4t1g1", "-m", "balance", "-po",
    // "0:0,1:0,2:0"};
    // Assert.assertTrue(tubectlTool.run(arg));
    // }

}
