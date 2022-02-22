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

package org.apache.inlong.sort.singletenant.flink.hive.filesystems;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.qcloud.chdfs.fs.CHDFSHadoopFileSystemAdapter;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.inlong.sort.configuration.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CHDFSFsFactory implements FileSystemFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CHDFSFsFactory.class);

    /**
     * Flink's configuration object.
     */
    private Configuration flinkConfig;

    /**
     * Hadoop's configuration for the file systems.
     */
    private org.apache.hadoop.conf.Configuration hadoopConfig;

    @Override
    public String getScheme() {
        return "ofs";
    }

    @Override
    public void configure(Configuration config) {
        flinkConfig = config;
        hadoopConfig = null; // reset the Hadoop Config
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        checkNotNull(fsUri);
        final String scheme = fsUri.getScheme();
        checkArgument(scheme != null, "file system has null scheme");

        try {
            // -- (1) get the loaded Hadoop config
            final org.apache.hadoop.conf.Configuration hadoopConfig;
            if (this.hadoopConfig != null) {
                hadoopConfig = this.hadoopConfig;
            } else if (flinkConfig != null) {
                hadoopConfig = new org.apache.hadoop.conf.Configuration();
                for (String key : this.flinkConfig.keySet()) {
                    if (key.startsWith(Constants.CHDFS_CONFIG_PREFIX)) {
                        String value = this.flinkConfig.getString(key, null);
                        hadoopConfig.set(key, value);
                        LOG.debug("Adding Flink config entry for {} as {} to Hadoop config",
                                key, hadoopConfig.get(key));
                    }
                }
            } else {
                LOG.warn("Hadoop configuration has not been explicitly initialized prior to loading"
                        + " a Hadoop file system. Using configuration from the classpath.");

                hadoopConfig = new org.apache.hadoop.conf.Configuration();
            }

            // -- (2) instantiate the Hadoop file system
            final org.apache.hadoop.fs.FileSystem hadoopFs = new CHDFSHadoopFileSystemAdapter();

            // -- (3) configure the Hadoop file system
            try {
                hadoopFs.initialize(fsUri, hadoopConfig);
            } catch (UnknownHostException e) {
                String message =
                        "The Hadoop file system's authority ("
                                + fsUri.getAuthority()
                                + "), specified by either the file URI or the configuration, cannot be resolved.";

                throw new IOException(message, e);
            }

            return new HadoopFileSystem(hadoopFs);

        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Cannot instantiate file system for URI: " + fsUri, e);
        }
    }
}
