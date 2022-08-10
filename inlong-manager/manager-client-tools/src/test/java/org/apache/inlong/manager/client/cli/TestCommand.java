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

package org.apache.inlong.manager.client.cli;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * Command service test for {@link CommandToolMain}
 */
@Slf4j
public class TestCommand {

    CommandToolMain inlongAdminTool = new CommandToolMain();


    @Test
    public void blankTest() {
        log.info("client tools cannot run the unit tests, as the application.properties not exist");
    }

    @Test
    public void testListGroup() {
        String[] arg = {"list", "group"};
        Assertions.assertTrue(inlongAdminTool.run(arg));
    }

    @Test
    public void testDescribeGroup() {
        String[] arg = {"describe", "group", "-g", "test", "-s", "130"};
        Assertions.assertTrue(inlongAdminTool.run(arg));
    }
    @Test
    public void testCreateGroup() {
        String[] arg = {"create", "group", "-f", "src/test/resources/create_group.json"};
        Assertions.assertTrue(inlongAdminTool.run(arg));
    }

    @Test
    public void testDeleteGroup(){
        String[] arg = {"delete", "group", "-id", "test_group"};
        Assertions.assertTrue(inlongAdminTool.run(arg));
    }

    @Test
    public void testUpdateGroup(){
        String[] arg = {"update", "group", "-id", "test_group", "-c", "src/test/resources/test_config.json"};
        Assertions.assertTrue(inlongAdminTool.run(arg));
    }

    @TestConfiguration
    public static class ClientUtils {
        public InlongClient getInlongClient() throws Exception {
            return getMockClient();
        }
    }

    private static InlongClient getMockClient() throws Exception {
        InlongClient inlongClient  = Mockito.mock(InlongClient.class);
        InlongGroup group = Mockito.mock(InlongGroup.class);
        Mockito.when(inlongClient.getGroup("test_group")).thenReturn(group);
        //Mockito.when(localMockRepository.count()).thenReturn(111L);
        return inlongClient;
    }
}
