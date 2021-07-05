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

package org.apache.inlong.agent.plugin.utils;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {

    public static String getTestTriggerProfile() {
        return "{\n"
            + "  \"job\": {\n"
            + "    \"dir\": {\n"
            + "      \"path\": \"\",\n"
            + "      \"pattern\": \"/AgentBaseTestsHelper/"
            + "org.apache.tubemq.inlong.plugin.fetcher.TestTdmFetcher/test[0-9].dat\"\n"
            + "    },\n"
            + "    \"trigger\": \"org.apache.inlong.agent.plugin.trigger.DirectoryTrigger\",\n"
            + "    \"id\": 1,\n"
            + "    \"thread\" : {\n"
            + "\"running\": {\n"
            + "\"core\": \"4\"\n"
            + "}\n"
            + "},  \n"
            + "    \"op\": \"0\",\n"
            + "    \"ip\": \"127.0.0.1\",\n"
            + "    \"name\": \"fileAgentTest\",\n"
            + "    \"source\": \"org.apache.inlong.agent.plugin.sources.TextFileSource\",\n"
            + "    \"sink\": \"org.apache.inlong.agent.plugin.sinks.MockSink\",\n"
            + "    \"channel\": \"org.apache.inlong.agent.plugin.channel.MemoryChannel\",\n"
            + "    \"standalone\": true,\n"
            + "    \"additionStr\": \"m=15&file=test\",\n"
            + "    \"deliveryTime\": \"1231313\",\n"
            + "    \"splitter\": \"&\"\n"
            + "  }\n"
            + "  }";
    }


    public static void createHugeFiles(String fileName, String rootDir, String record) throws Exception {
        final Path hugeFile = Paths.get(rootDir, fileName);
        FileWriter writer = new FileWriter(hugeFile.toFile());
        for (int i = 0; i < 1024; i++) {
            writer.write(record);
        }
        writer.flush();
        writer.close();
    }



    public static void createMultipleLineFiles(String fileName, String rootDir,
        String record, int lineNum) throws Exception {
        final Path hugeFile = Paths.get(rootDir, fileName);
        List<String> beforeList = new ArrayList<>();
        for (int i = 0; i < lineNum; i++) {
            beforeList.add(String.format("%s_%d", record, i));
        }
        Files.write(hugeFile, beforeList, StandardOpenOption.CREATE);
    }
}
