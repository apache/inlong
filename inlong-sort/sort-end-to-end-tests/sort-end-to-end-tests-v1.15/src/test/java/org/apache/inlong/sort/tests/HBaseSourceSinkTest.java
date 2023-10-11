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

package org.apache.inlong.sort.tests;

import org.apache.inlong.sort.tests.utils.FlinkContainerTestEnv;
import org.apache.inlong.sort.tests.utils.HBaseContainer;
import org.apache.inlong.sort.tests.utils.TestUtils;

import org.apache.flink.util.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zfancy
 * @version 1.0
 */
public class HBaseSourceSinkTest extends FlinkContainerTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSourceSinkTest.class);

    private static final Path HADOOP_CP = TestUtils.getResource(".*hadoop.classpath");
    private static final Path hbaseJar = TestUtils.getResource("sort-connector-hbase.jar");
    // private static final Path hbaseJar = TestUtils.getResource("sort-connector-hbase-v1.15.jar");
    // Can't use getResource("xxx").getPath(), windows will don't know that path
    private static final String sqlFile;

    private List<Path> hadoopCpJars;

    static {
        try {
            sqlFile = Paths.get(HBaseSourceSinkTest.class.getResource("/flinkSql/hbase_e2e.sql").toURI()).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static final HBaseContainer HBASE_SOURCE_CONTAINER = (HBaseContainer) new HBaseContainer(
            "2.2.3")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("hbase1")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final HBaseContainer HBASE_SINK_CONTAINER = (HBaseContainer) new HBaseContainer(
            "2.2.3")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("hbase2")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Before
    public void setup() throws IOException {
        File hadoopClasspathFile = new File(HADOOP_CP.toAbsolutePath().toString());
        if (!hadoopClasspathFile.exists()) {
            throw new FileNotFoundException(
                    "File that contains hadoop classpath " + HADOOP_CP + " does not exist.");
        }
        String classPathContent = FileUtils.readFileUtf8(hadoopClasspathFile);
        hadoopCpJars =
                Arrays.stream(classPathContent.split(":"))
                        .map(Paths::get)
                        .collect(Collectors.toList());

        // HBASE_SOURCE_CONTAINER.start();
        // HBASE_SINK_CONTAINER.start();
        waitUntilJobRunning(Duration.ofSeconds(30));
        initializeHBaseTable();
    }

    @AfterClass
    public static void teardown() {
        if (HBASE_SOURCE_CONTAINER != null) {
            HBASE_SOURCE_CONTAINER.stop();
        }
        if (HBASE_SINK_CONTAINER != null) {
            HBASE_SINK_CONTAINER.stop();
        }
    }

    private void initializeHBaseTable() {
        try {
            // HBASE_SOURCE_CONTAINER.start();
            // HBASE_SINK_CONTAINER.start();
            HBASE_SOURCE_CONTAINER.createTable("sourceTable", "family1", "family2");
            // HBASE_SINK_CONTAINER.createTable("sinkTable", "family1", "family2");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testHBaseSourceAndSink() throws Exception {
        //still have some confusing problem
        HBASE_SOURCE_CONTAINER.putData("sourceTable", "row1", "family1", "f1c1", "v1");
        HBASE_SOURCE_CONTAINER.putData("sourceTable", "row1", "family2", "f2c1", "v2");
        HBASE_SOURCE_CONTAINER.putData("sourceTable", "row1", "family2", "f2c2", "v3");
        HBASE_SOURCE_CONTAINER.putData("sourceTable", "row2", "family1", "f1c1", "v4");
        HBASE_SOURCE_CONTAINER.putData("sourceTable", "row2", "family2", "f2c1", "v5");
        HBASE_SOURCE_CONTAINER.putData("sourceTable", "row2", "family2", "f2c2", "v6");

        Path[] jarPaths = new Path[hadoopCpJars.size() + 1];
        jarPaths[0] = hbaseJar;
        for (int i = 0; i < hadoopCpJars.size(); i++) {
            jarPaths[i + 1] = hadoopCpJars.get(i);
        }
        submitSQLJob(sqlFile, jarPaths);
        waitUntilJobRunning(Duration.ofSeconds(10));
    }

}
