/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;

/**
 * common environment setting up for test cases.
 */
public class AgentBaseTestsHelper {

    private static Path testRootDir;

    public static void setupAgentHome() throws Exception {
        testRootDir = Paths
                .get(FileUtils.getTempDirectoryPath(), AgentBaseTestsHelper.class.getSimpleName());
        if (Files.isWritable(testRootDir)) {
            FileUtils.forceDelete(testRootDir.toFile());
        }
        FileUtils.forceMkdir(testRootDir.toFile());
        System.setProperty("agent.home", getTestRootDir().toString());
    }

    public static void teardownAgentHome() throws Exception {
        FileUtils.forceDelete(testRootDir.toFile());
    }

    public static Path getTestRootDir() {
        return testRootDir;
    }
}
