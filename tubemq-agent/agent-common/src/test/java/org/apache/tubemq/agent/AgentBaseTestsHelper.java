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

import static org.apache.tubemq.agent.constants.AgentConstants.AGENT_HOME;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.tubemq.agent.conf.AgentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * common environment setting up for test cases.
 */
public class AgentBaseTestsHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentBaseTestsHelper.class);

    private static final AtomicLong fileIndex = new AtomicLong();
    private static final ConcurrentHashMap<String, Path> localPath = new ConcurrentHashMap<>();

    public static void setupAgentHome(String className) {
        long uniqId = fileIndex.incrementAndGet();
        Path testRootDir = Paths
            .get("/tmp", AgentBaseTestsHelper.class.getSimpleName(), String.valueOf(uniqId));
        localPath.putIfAbsent(className, testRootDir);
        teardownAgentHome(className);
        boolean result = testRootDir.toFile().mkdirs();
        LOGGER.info("try to create {}, result is {}", testRootDir, result);

        AgentConfiguration.getAgentConf().set(AGENT_HOME, testRootDir.toString());
    }

    public static Path getTestRootDir(String className) {
        return localPath.get(className);
    }

    public static void teardownAgentHome(String className) {
        Path testRootDir = localPath.get(className);
        if (testRootDir != null) {
            try {
                FileUtils.deleteDirectory(testRootDir.toFile());
            } catch (Exception ignored) {

            }
        }
    }
}
