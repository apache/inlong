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

package org.apache.inlong.sort.flink.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.inlong.sort.ZkTools;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.flink.hive.partition.HivePartition;
import org.apache.inlong.sort.flink.hive.partition.PartitionCommitPolicy;
import org.apache.inlong.sort.flink.transformation.RecordTransformer;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.meta.MetaManager;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.CsvDeserializationInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveFieldPartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveTimePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.TextFileFormat;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.apache.inlong.sort.util.TestLogger;
import org.apache.inlong.sort.util.ZooKeeperTestEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSinkITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(HiveSinkITCase.class);

    private static final CountDownLatch verificationFinishedLatch = new CountDownLatch(1);

    private static final CountDownLatch jobFinishedLatch = new CountDownLatch(1);

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private final String cluster = "cluster";

    private final String zkRoot = "/test";

    private static DataFlowInfo dataFlowInfo;

    private static ZooKeeperTestEnvironment ZOOKEEPER;

    private final org.apache.inlong.sort.configuration.Configuration config =
            new org.apache.inlong.sort.configuration.Configuration();

    private FileSystem dfs;

    private String dfsSchema;

    private MiniDFSCluster hdfsCluster;

    private final String hiveMetastoreUrl = "127.0.0.1:10000";

    private String hdfsDataDir;

    private final String hadoopProxyUser = "somebody";

    private final String hiveDb = "hive_db";

    private final String hiveTable = "hive_table";

    private final String hiveUsername = "username";

    private final String hivePassword = "password";

    private static final String fieldName1 = "f1";

    private static final long fieldValue1 = 1624432767000L;

    private final String fieldName2 = "f2";

    private static final int fieldValue2 = 9527;

    private final String fieldName3 = "f3";

    private final String fieldName4 = "f4";

    private static final String[][] fileContents = new String[][]{{"abc", "def"}, {"123", "456"}};

    private final String timePartitionFormat = "yyyyMMddHH";

    private final String timePartitionValue = new SimpleDateFormat(timePartitionFormat).format(new Date(fieldValue1));

    @Before
    public void setUp() throws Exception {
        ZOOKEEPER = new ZooKeeperTestEnvironment(1);
        initializeHdfs();
        dataFlowInfo = prepareDataFlowSchema();
        prepareZookeeperSchema();
    }

    @After
    public void cleanUp() throws Exception {
        MetaManager.release();
        ZOOKEEPER.shutdown();
        showdownHdfs();
    }

    private void prepareZookeeperSchema() throws Exception {
        config.setString(Constants.CLUSTER_ID, cluster);
        config.setString(Constants.ZOOKEEPER_QUORUM, ZOOKEEPER.getConnectString());
        config.setString(Constants.ZOOKEEPER_ROOT, zkRoot);

        ZkTools.addDataFlowToCluster(cluster, 1, ZOOKEEPER.getConnectString(), zkRoot);
        ZkTools.updateDataFlowInfo(
                dataFlowInfo,
                cluster,
                dataFlowInfo.getId(),
                ZOOKEEPER.getConnectString(),
                zkRoot);
    }

    private void initializeHdfs() throws IOException {
        final UserGroupInformation currentUser = UserGroupInformation.getLoginUser();

        Configuration conf = new Configuration();

        File dataDir = tempFolder.newFolder();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
        conf.set("hadoop.proxyuser." + currentUser.getUserName() + ".groups", "*");
        conf.set("hadoop.proxyuser." + currentUser.getUserName() + ".hosts", "*");

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();

        dfs = hdfsCluster.getFileSystem();

        dfsSchema = "hdfs://"
                + NetUtils.hostAndPortToUrlString(hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort());

        hdfsDataDir = "/user/u_teg_tdbank/hive_test_root/" + hiveDb + ".db/" + hiveTable;

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

    private HiveSinkInfo prepareSinkSchema() {
        final FieldInfo f1 = new FieldInfo(fieldName1, new TimestampFormatInfo("MILLIS"));
        final FieldInfo f2 = new FieldInfo(fieldName2, IntFormatInfo.INSTANCE);
        final FieldInfo f3 = new FieldInfo(fieldName3, StringFormatInfo.INSTANCE);
        final FieldInfo f4 = new FieldInfo(fieldName4, StringFormatInfo.INSTANCE);

        final HiveTimePartitionInfo timePartition
                = new HiveTimePartitionInfo(f1.getName(), timePartitionFormat);
        final HiveFieldPartitionInfo fieldPartition = new HiveFieldPartitionInfo(f2.getName());

        return new HiveSinkInfo(
                new FieldInfo[]{f1, f2, f3, f4},
                hiveMetastoreUrl,
                hiveDb,
                hiveTable,
                hiveUsername,
                hivePassword,
                dfsSchema + hdfsDataDir,
                new HivePartitionInfo[]{timePartition, fieldPartition},
                new TextFileFormat("\t".charAt(0))
        );
    }

    private DataFlowInfo prepareDataFlowSchema() {
        return new DataFlowInfo(9527L, prepareSourceSchema(), prepareSinkSchema());
    }

    private SourceInfo prepareSourceSchema() {
        CsvDeserializationInfo deserializationInfo = new CsvDeserializationInfo("\t".charAt(0));
        return new TubeSourceInfo(
                "topic",
                "127.0.0.1:12345",
                null,
                deserializationInfo,
                new FieldInfo[0]);
    }

    @Test(timeout = 60000)
    public void test() throws Exception {
        config.setLong(Constants.SINK_HIVE_ROLLING_POLICY_CHECK_INTERVAL, 1000L);
        config.setLong(Constants.SINK_HIVE_ROLLING_POLICY_ROLLOVER_INTERVAL, 1000L);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//            env.enableCheckpointing(1000L);
//            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(UNLIMITED_TOLERABLE_FAILURE_NUMBER);
            env.setRestartStrategy(RestartStrategies.noRestart());
            env.addSource(new TestingSourceFunction())
                    .setParallelism(1)
                    .process(new HiveMultiTenantWriter(config))
                    .setParallelism(1)
                    .process(new HiveMultiTenantCommitter(config, new TestingPartitionCommitPolicyFactory()))
                    .setParallelism(1);
            try {
                env.execute(); // will blocking here
            } catch (Exception e) {
                LOG.error("Unexpected exception thrown", e);
            } finally {
                jobFinishedLatch.countDown();
            }
        });

        try {
            boolean fileVerified = false;
            boolean partitionVerified = false;
            while (true) {
                if (!fileVerified) {
                    fileVerified = verifyHdfsFile();
                }
                if (!partitionVerified) {
                    partitionVerified = verifyCommittedPartitions();
                }
                if (fileVerified && partitionVerified) {
                    break;
                } else {
                    LOG.info("File verification status {}, partition verification status {},"
                            + " would retry later", fileVerified, partitionVerified);
                    //noinspection BusyWait
                    Thread.sleep(1000);
                }
            }
            verificationFinishedLatch.countDown();
            jobFinishedLatch.await();
        } finally {
            executorService.shutdown();
        }
    }

    private boolean verifyHdfsFile() throws IOException {
        final FileStatus[] tableDirFileStatuses = dfs.listStatus(new Path(hdfsDataDir));
        if (tableDirFileStatuses == null || tableDirFileStatuses.length < 1) {
            return false;
        }
        // check primary partition dir
        assertEquals(1, tableDirFileStatuses.length);
        final FileStatus primaryPartitionFileStatus = tableDirFileStatuses[0];
        final Path primaryPartitionPath = primaryPartitionFileStatus.getPath();
        final String primaryPartitionName = primaryPartitionPath.getName();
        assertEquals(fieldName1 + "=" + timePartitionValue, primaryPartitionName);
        // check secondary partition dir
        final FileStatus[] primaryPartitionDirFileStatus = dfs.listStatus(primaryPartitionPath);
        if (primaryPartitionDirFileStatus == null || primaryPartitionDirFileStatus.length < 1) {
            return false;
        }
        assertEquals(1, primaryPartitionDirFileStatus.length);
        final Path secondaryPartitionPath = primaryPartitionDirFileStatus[0].getPath();
        final String secondaryPartitionName = secondaryPartitionPath.getName();
        assertEquals(fieldName2 + "=" + fieldValue2, secondaryPartitionName);
        // check data file
        final FileStatus[] secondaryPartitionDirFileStatus = dfs.listStatus(secondaryPartitionPath);
        if (secondaryPartitionDirFileStatus == null || secondaryPartitionDirFileStatus.length < 1) {
            return false;
        }
        final FileStatus dataFileStatus = secondaryPartitionDirFileStatus[0];
        if (dataFileStatus.getLen() <= 0) {
            return false;
        }
        final Path dataFilePath = dataFileStatus.getPath();
        final FSDataInputStream inputStream = dfs.open(dataFilePath);
        final List<String> results = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                results.add(line);
            }
        }
        final List<String> lineContents = new ArrayList<>(fileContents.length);
        for (String[] line : fileContents) {
            lineContents.add(StringUtils.join(line, "\t"));
        }
        assertEquals(StringUtils.join(lineContents, "\n"), StringUtils.join(results, "\n"));
        return true;
    }

    private boolean verifyCommittedPartitions() {
        synchronized (TestingHivePartitionCommitPolicy.hivePartitionInfos) {
            if (TestingHivePartitionCommitPolicy.hivePartitionInfos.isEmpty()) {
                return false;
            } else {
                TestingHivePartitionInfo partitionInfo = TestingHivePartitionCommitPolicy.hivePartitionInfos.get(0);
                assertEquals(hiveMetastoreUrl, partitionInfo.metastoreUrl);
                assertEquals(hiveDb, partitionInfo.database);
                assertEquals(hiveTable, partitionInfo.table);
                assertEquals(hiveUsername, partitionInfo.username);
                assertEquals(hivePassword, partitionInfo.password);
                assertEquals(2, partitionInfo.hivePartition.getPartitions().length);
                assertEquals(fieldName1, partitionInfo.hivePartition.getPartitions()[0].f0);
                assertEquals(timePartitionValue, partitionInfo.hivePartition.getPartitions()[0].f1);
                assertEquals(fieldName2, partitionInfo.hivePartition.getPartitions()[1].f0);
                assertEquals(Integer.toString(fieldValue2), partitionInfo.hivePartition.getPartitions()[1].f1);
                return true;
            }
        }
    }

    private static class TestingSourceFunction extends RichSourceFunction<SerializedRecord> {

        private static final long serialVersionUID = -5969057934139454695L;

        private transient RecordTransformer transformer;

        @Override
        public void open(org.apache.flink.configuration.Configuration configuration) throws Exception {
            transformer = new RecordTransformer(1024);
            transformer.addDataFlow(dataFlowInfo);
        }

        @Override
        public void run(SourceContext<SerializedRecord> sourceContext) throws Exception {
            for (String[] line : fileContents) {
                final Row row = new Row(4);
                // the first field is time based partition
                row.setField(0, new Timestamp(fieldValue1 + new Random().nextInt(999)));
                row.setField(1, fieldValue2);
                row.setField(2, line[0]);
                row.setField(3, line[1]);
                sourceContext.collect(transformer.toSerializedRecord(new Record(dataFlowInfo.getId(), row)));
            }
            verificationFinishedLatch.await();
        }

        @Override
        public void cancel() {

        }
    }

    private static class TestingHivePartitionCommitPolicy implements PartitionCommitPolicy {

        private final HiveSinkInfo hiveSinkInfo;

        private static final List<TestingHivePartitionInfo> hivePartitionInfos = new ArrayList<>();

        public TestingHivePartitionCommitPolicy(HiveSinkInfo hiveSinkInfo) {
            this.hiveSinkInfo = hiveSinkInfo;
        }

        @Override
        public void commit(Context context) {
            synchronized (hivePartitionInfos) {
                hivePartitionInfos.add(
                        new TestingHivePartitionInfo(
                                hiveSinkInfo.getHiveServerJdbcUrl(),
                                context.databaseName(),
                                context.tableName(),
                                hiveSinkInfo.getUsername(),
                                hiveSinkInfo.getPassword(),
                                context.partition()
                        ));
            }
        }

        @Override
        public void close() {
            synchronized (hivePartitionInfos) {
                hivePartitionInfos.clear();
            }
        }
    }

    private static class TestingHivePartitionInfo {

        private final String metastoreUrl;
        private final String database;
        private final String table;
        private final String username;
        private final String password;
        private final HivePartition hivePartition;

        private TestingHivePartitionInfo(
                String metastoreUrl,
                String database,
                String table,
                String username,
                String password,
                HivePartition hivePartition) {
            this.metastoreUrl = metastoreUrl;
            this.database = database;
            this.table = table;
            this.username = username;
            this.password = password;
            this.hivePartition = hivePartition;
        }
    }

    private static class TestingPartitionCommitPolicyFactory implements PartitionCommitPolicy.Factory {

        private static final long serialVersionUID = -6718905736895264919L;

        @Override
        public PartitionCommitPolicy create(org.apache.inlong.sort.configuration.Configuration configuration,
                HiveSinkInfo hiveSinkInfo) {
            return new TestingHivePartitionCommitPolicy(hiveSinkInfo);
        }
    }
}
