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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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
                long fileArchiveDelayTime = currentTime - context.getFileArchiveDelay();
                for (FileStatus fileStatus : fileStatusArray) {
                    // check all file that have overtimed.
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
        for (FileStatus fileStatus : intmpFiles) {
            Path intmpFilePath = fileStatus.getPath();
            String strIntmpFullFile = intmpFilePath.getName();
            int index = strIntmpFullFile.lastIndexOf('/');
            String strIntmpFile = strIntmpFullFile.substring(index + 1);
            Path inFilePath = new Path(idFile.getInPath(), strIntmpFile);
            fs.rename(intmpFilePath, inFilePath);
        }

        // clear "outtmp" directory.
        FileStatus[] outtmpFiles = fs.listStatus(idFile.getOuttmpPath());
        for (FileStatus fileStatus : outtmpFiles) {
            fs.delete(fileStatus.getPath(), true);
        }

        // merge and copy files in "in" directory to "outtmp" directory.
        FileStatus[] inFiles = fs.listStatus(idFile.getInPath());
        long outputFileSize = 0;
        List<Path> concatInFiles = new ArrayList<>();
        for (FileStatus fileStatus : inFiles) {
            if (outputFileSize < context.getMaxOutputFileSize()) {
                concatInFiles.add(fileStatus.getPath());
                continue;
            }
            Path outputFilePath = new Path(idFile.getOutPath(), context.getNodeId() + "." + System.currentTimeMillis());
            Path[] paths = new Path[concatInFiles.size()];
            fs.concat(outputFilePath, concatInFiles.toArray(paths));
            outputFileSize = 0;
            concatInFiles.clear();
        }
        if (concatInFiles.size() > 0) {
            Path outputFilePath = new Path(idFile.getOutPath(), context.getNodeId() + "." + System.currentTimeMillis());
            Path[] paths = new Path[concatInFiles.size()];
            fs.concat(outputFilePath, concatInFiles.toArray(paths));
        }

        // rename outtmp file to "out" directory.
        outtmpFiles = fs.listStatus(idFile.getOuttmpPath());
        for (FileStatus fileStatus : outtmpFiles) {
            Path outtmpFilePath = fileStatus.getPath();
            String strOuttmpFullFile = outtmpFilePath.getName();
            int index = strOuttmpFullFile.lastIndexOf('/');
            String strOuttmpFile = strOuttmpFullFile.substring(index + 1);
            Path outFilePath = new Path(idFile.getOutPath(), strOuttmpFile);
            fs.rename(outtmpFilePath, outFilePath);
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
