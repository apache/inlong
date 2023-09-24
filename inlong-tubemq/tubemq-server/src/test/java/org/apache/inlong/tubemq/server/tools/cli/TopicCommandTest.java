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

public class TopicCommandTest {

    CommandToolMain tubectlTool = null;

    @Before
    public void setUp() {
        tubectlTool = new CommandToolMain();
    }

    @Test
    public void testTopicCreate() {
        String[] arg = {"topic", "create", "-t", "b4t1", "-bid", "4", "-c", "admin", "-at", "abc"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testTopicList() {
        String[] arg = {"topic", "list"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testTopicUpdate() {
        String[] arg = {"topic", "update", "-t", "b4t1", "-bid", "4", "-m", "admin", "-at", "abc"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testTopicDeleteSoft() {
        String[] arg = {"topic", "delete", "-o", "soft", "-t", "b4t1", "-bid", "4", "-m", "admin", "-at", "abc"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testTopicDeleteRedo() {
        String[] arg = {"topic", "delete", "-o", "redo", "-t", "b4t1", "-bid", "4", "-m", "admin", "-at", "abc"};
        Assert.assertTrue(tubectlTool.run(arg));
    }

    @Test
    public void testTopicDeleteHard() {
        String[] arg = {"topic", "delete", "-o", "hard", "-t", "b4t1", "-bid", "4", "-m", "admin", "-at", "abc"};
        Assert.assertTrue(tubectlTool.run(arg));
    }
}
