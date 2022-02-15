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

package org.apache.inlong.sort.singletenant.flink.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.NetUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.hive.HiveWriter;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.TextFileFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSinkWithoutPartitionTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(HiveSinkWithoutPartitionTestCase.class);

    private static final CountDownLatch verificationFinishedLatch = new CountDownLatch(1);

    private static final CountDownLatch jobFinishedLatch = new CountDownLatch(1);

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static final long dataflowId = 9527;

    private final org.apache.inlong.sort.configuration.Configuration config =
            new org.apache.inlong.sort.configuration.Configuration();

    private FileSystem dfs;

    private String dfsSchema;

    private MiniDFSCluster hdfsCluster;

    private String hdfsDataDir;

    private final String hiveDb = "hive_db";

    private final String hiveTable = "hive_table";

    @Before
    public void setUp() throws Exception {
        initializeHdfs();
    }

    @After
    public void cleanUp() {
        showdownHdfs();
    }

    @Test(timeout = 60000)
    public void test() throws Exception {
        config.setLong(Constants.SINK_HIVE_ROLLING_POLICY_CHECK_INTERVAL, 1000L);
        config.setLong(Constants.SINK_HIVE_ROLLING_POLICY_ROLLOVER_INTERVAL, 1000L);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(1000L);
            env.setRestartStrategy(RestartStrategies.noRestart());
            env.addSource(new TestingSourceFunction())
                    .setParallelism(1)
                    .process(new HiveWriter(config, dataflowId, prepareSinkInfo()))
                    .setParallelism(1);
            try {
                env.execute(); // will block here
            } catch (Exception e) {
                LOG.error("Unexpected exception thrown", e);
            } finally {
                jobFinishedLatch.countDown();
            }
        });

        try {
            boolean fileVerified = false;

            while (true) {
                if (!fileVerified) {
                    fileVerified = verifyHdfsFile();
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } else {
                    break;
                }
            }
            verificationFinishedLatch.countDown();
            jobFinishedLatch.await();
        } finally {
            executorService.shutdown();
        }
    }

    private void initializeHdfs() throws IOException {
        final File dataDir = tempFolder.newFolder();
        final UserGroupInformation currentUser = UserGroupInformation.getLoginUser();

        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
        conf.set("hadoop.proxyuser." + currentUser.getUserName() + ".groups", "*");
        conf.set("hadoop.proxyuser." + currentUser.getUserName() + ".hosts", "*");

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();

        dfs = hdfsCluster.getFileSystem();
        dfsSchema = "hdfs://" + NetUtils.hostAndPortToUrlString(
                hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort());
        hdfsDataDir = "/user/test_user/root_path/" + hiveDb + ".db/" + hiveTable;

        prepareHdfsPath();
    }

    private void showdownHdfs() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
            hdfsCluster = null;
        }
    }

    private void prepareHdfsPath() throws IOException {
        final Path hdfsRootPath = new Path(hdfsDataDir);
        assertTrue(dfs.mkdirs(hdfsRootPath));
        dfs.setPermission(hdfsRootPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }

    private HiveSinkInfo prepareSinkInfo() {
        return new HiveSinkInfo(
                new FieldInfo[]{
                        new FieldInfo("f1", StringFormatInfo.INSTANCE),
                        new FieldInfo("f2", IntFormatInfo.INSTANCE)
                },
                "127.0.0.1:10000",
                hiveDb,
                hiveTable,
                "username",
                "password",
                dfsSchema + hdfsDataDir,
                new HivePartitionInfo[]{},
                new TextFileFormat("\t".charAt(0))
        );
    }

    private boolean verifyHdfsFile() throws IOException {
        final FileStatus[] fileStatuses = dfs.listStatus(new Path(hdfsDataDir));
        if (fileStatuses == null || fileStatuses.length < 1) {
            return false;
        }

        final List<String> results = new ArrayList<>();
        for (FileStatus dataFileStatus : fileStatuses) {
            final Path dataFilePath = dataFileStatus.getPath();

            if (dataFileStatus.getLen() <= 0 || dataFilePath.getName().startsWith(".")) {
                // File name starts with . means that the file is an in-progress file
                continue;
            }

            // Read data files
            final FSDataInputStream inputStream = dfs.open(dataFilePath);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    results.add(line);
                }
            }
        }

        // Verify results
        if (results.size() < 10) {
            return false;
        }

        Collections.sort(results);
        for (int i = 0; i < results.size(); ++i) {
            assertEquals("name_" + i + "\t" + i, results.get(i));
        }
        return true;
    }

    private static class TestingSourceFunction extends RichSourceFunction<Row> {

        private static final long serialVersionUID = -5969057934139454695L;

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {
            for (int i = 0; i < 10; i++) {
                sourceContext.collect(Row.of("name_" + i, i));
            }
            verificationFinishedLatch.await();
        }

        @Override
        public void cancel() {

        }
    }
}
