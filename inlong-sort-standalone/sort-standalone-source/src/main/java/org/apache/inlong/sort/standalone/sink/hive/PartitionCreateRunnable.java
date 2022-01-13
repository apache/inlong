/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.standalone.sink.hive;

import static org.apache.inlong.sort.standalone.sink.hive.HiveSinkContext.MINUTE_MS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.slf4j.Logger;

/**
 * 
 * PartitionCreateRunnable
 */
public class PartitionCreateRunnable implements Runnable {

    public static final Logger LOG = InlongLoggerFactory.getLogger(PartitionCreateRunnable.class);

    private final HiveSinkContext context;
    private final HdfsIdConfig idConfig;
    private final String strPartitionValue;
    private final long partitionTime;
    private boolean isForce;
    private PartitionState state;

    /**
     * Constructor
     * 
     * @param context
     * @param idConfig
     * @param strPartitionValue
     * @param partitionTime
     * @param isForce
     */
    public PartitionCreateRunnable(HiveSinkContext context, HdfsIdConfig idConfig, String strPartitionValue,
            long partitionTime, boolean isForce) {
        this.context = context;
        this.idConfig = idConfig;
        this.strPartitionValue = strPartitionValue;
        this.partitionTime = partitionTime;
        this.isForce = isForce;
        this.state = PartitionState.INIT;
    }

    /**
     * run
     */
    @Override
    public void run() {
        LOG.info("start to PartitionCreateRunnable.");
        this.state = PartitionState.CREATING;
        HdfsIdFile idFile = null;
        try {
            String strIdRootPath = idConfig.parsePartitionPath(partitionTime);
            idFile = new HdfsIdFile(context, idConfig, strIdRootPath);
            // force to close partition that has overtimed.
            if (isForce) {
                this.process(idFile);
            } else {
                // try to close partition that has no new data and can be closed.
                FileSystem fs = idFile.getFs();
                FileStatus[] fileStatusArray = fs.listStatus(new Path[]{idFile.getIntmpPath(), idFile.getInPath()});
                long currentTime = System.currentTimeMillis();
                long fileArchiveDelayTime = currentTime
                        - context.getFileArchiveDelayMinute() * MINUTE_MS;
                LOG.info("start to PartitionCreateRunnable check currentTime:{},fileArchiveDelayTime:{},"
                        + "FileArchiveDelayMinute:{},MINUTE_MS:{}",
                        currentTime, fileArchiveDelayTime, context.getFileArchiveDelayMinute(), MINUTE_MS);
                for (FileStatus fileStatus : fileStatusArray) {
                    // check all file that have overtimed.
                    LOG.info("start to PartitionCreateRunnable check fileStatus.getModificationTime():{},"
                            + "fileArchiveDelayTime:{}", fileStatus.getModificationTime(), fileArchiveDelayTime);
                    if (fileStatus.getModificationTime() > fileArchiveDelayTime) {
                        this.state = PartitionState.ERROR;
                        return;
                    }
                }
                this.process(idFile);
            }
            this.state = PartitionState.CREATED;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            this.state = PartitionState.ERROR;
        } finally {
            if (idFile != null) {
                idFile.close();
            }
        }
    }

    /**
     * process
     * 
     * @param  idFile
     * @throws IOException
     * @throws FileNotFoundException
     */
    private void process(HdfsIdFile idFile) throws FileNotFoundException, IOException {
        DistributedFileSystem fs = idFile.getFs();

        // rename files in "intmp" directory to "in" directory.
        FileStatus[] intmpFiles = fs.listStatus(idFile.getIntmpPath());
        long currentTime = System.currentTimeMillis();
        long fileArchiveDelayTime = currentTime
                - context.getFileArchiveDelayMinute() * MINUTE_MS;
        for (FileStatus fileStatus : intmpFiles) {
            if (fileStatus.getModificationTime() > fileArchiveDelayTime) {
                continue;
            }
            Path intmpFilePath = fileStatus.getPath();
            String strIntmpFullFile = intmpFilePath.getName();
            int index = strIntmpFullFile.lastIndexOf('/');
            String strIntmpFile = strIntmpFullFile.substring(index + 1);
            Path inFilePath = new Path(idFile.getInPath(), strIntmpFile);
            fs.rename(intmpFilePath, inFilePath);
        }

        // clear "outtmp" fiels.
        FileStatus[] inFiles = fs.listStatus(idFile.getInPath());
        for (FileStatus fileStatus : inFiles) {
            Path inFile = fileStatus.getPath();
            if (inFile.getName().lastIndexOf(HdfsIdFile.OUTTMP_FILE_POSTFIX) >= 0) {
                fs.delete(inFile, true);
            }
        }

        // merge and copy files in "in" directory to "outtmp" file.
        long outputFileSize = 0;
        List<Path> concatInFiles = new ArrayList<>();
        for (FileStatus fileStatus : inFiles) {
            if (fileStatus.getLen() <= 0) {
                continue;
            }
            if (outputFileSize < context.getMaxOutputFileSizeGb() * HiveSinkContext.GB_BYTES) {
                concatInFiles.add(fileStatus.getPath());
                continue;
            }
            this.concatInFiles2OuttmpFile(idFile, concatInFiles, fs);
            outputFileSize = 0;
            concatInFiles.clear();
        }
        if (concatInFiles.size() > 0) {
            this.concatInFiles2OuttmpFile(idFile, concatInFiles, fs);
            outputFileSize = 0;
            concatInFiles.clear();
        }

        // rename outtmp file to "out" directory.
        inFiles = fs.listStatus(idFile.getInPath());
        for (FileStatus fileStatus : inFiles) {
            Path inFile = fileStatus.getPath();
            if (inFile.getName().lastIndexOf(HdfsIdFile.OUTTMP_FILE_POSTFIX) >= 0) {
                String strFullFile = inFile.getName();
                int index = strFullFile.lastIndexOf('/');
                String strOuttmpFile = strFullFile.substring(index + 1,
                        strFullFile.length() - HdfsIdFile.OUTTMP_FILE_POSTFIX.length());
                Path outFilePath = new Path(idFile.getOutPath(), strOuttmpFile);
                fs.rename(inFile, outFilePath);
            }
        }

        // delete file in "in" directory.
        for (FileStatus fileStatus : inFiles) {
            fs.delete(fileStatus.getPath(), true);
        }

        // execute the sql of adding partition.
        try (Connection conn = context.getHiveConnection()) {
            Statement stat = conn.createStatement();
            String partitionSqlPattern = "ALTER TABLE %s.%s ADD IF NOT EXISTS PARTITION (dt='%s') LOCATION '%s'";
            String partitionSql = String.format(partitionSqlPattern,
                    context.getHiveDatabase(),
                    idConfig.getHiveTableName(),
                    this.strPartitionValue,
                    idFile.getOutPath().getName());
            stat.executeUpdate(partitionSql);
            stat.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * concatInFiles2OuttmpFile
     * 
     * @param  idFile
     * @param  concatInFiles
     * @param  fs
     * @throws IOException
     */
    private void concatInFiles2OuttmpFile(HdfsIdFile idFile, List<Path> concatInFiles, DistributedFileSystem fs)
            throws IOException {
        Path outtmpFilePath = new Path(idFile.getInPath(),
                HdfsIdFile.getFileName(context, System.currentTimeMillis()) + HdfsIdFile.OUTTMP_FILE_POSTFIX);
        FSDataOutputStream outputFileStream = fs.create(outtmpFilePath, true);
        outputFileStream.flush();
        outputFileStream.close();
        Path[] paths = new Path[concatInFiles.size()];
        fs.concat(outtmpFilePath, concatInFiles.toArray(paths));
    }

    /**
     * get state
     * 
     * @return the state
     */
    public PartitionState getState() {
        return state;
    }

    /**
     * set state
     * 
     * @param state the state to set
     */
    public void setState(PartitionState state) {
        this.state = state;
    }

    /**
     * get context
     * 
     * @return the context
     */
    public HiveSinkContext getContext() {
        return context;
    }

    /**
     * get idConfig
     * 
     * @return the idConfig
     */
    public HdfsIdConfig getIdConfig() {
        return idConfig;
    }

    /**
     * get strPartitionValue
     * 
     * @return the strPartitionValue
     */
    public String getStrPartitionValue() {
        return strPartitionValue;
    }

    /**
     * get isForce
     * 
     * @return the isForce
     */
    public boolean isForce() {
        return isForce;
    }

    /**
     * get partitionTime
     * 
     * @return the partitionTime
     */
    public long getPartitionTime() {
        return partitionTime;
    }

    /**
     * set isForce
     * 
     * @param isForce the isForce to set
     */
    public void setForce(boolean isForce) {
        this.isForce = isForce;
    }

}
