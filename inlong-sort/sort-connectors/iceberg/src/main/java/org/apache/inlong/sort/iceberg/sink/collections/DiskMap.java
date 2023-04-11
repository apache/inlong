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

package org.apache.inlong.sort.iceberg.sink.collections;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public abstract class DiskMap<T, R> implements Map<T, R>, Iterable<R> {

    private static final Logger LOG = LoggerFactory.getLogger(DiskMap.class);
    private final File diskMapPathFile;
    private transient Thread shutdownThread = null;
    protected final String diskMapPath;
    protected final MapStateDescriptor<T, R> descriptor;

    public DiskMap(MapStateDescriptor<T, R> descriptor, String basePath, String prefix) throws IOException {
        this.descriptor = descriptor;
        this.diskMapPath = String.format("%s/%s-%s", basePath, prefix, UUID.randomUUID());
        this.diskMapPathFile = new File(this.diskMapPath);
        LOG.info("Open rocksdb dir in {}", diskMapPath);
        FileIOUtils.deleteDirectory(this.diskMapPathFile);
        FileIOUtils.mkdir(this.diskMapPathFile);
        this.diskMapPathFile.deleteOnExit();
        this.addShutDownHook();
    }

    private void addShutDownHook() {
        this.shutdownThread = new Thread(this::cleanup);
        Runtime.getRuntime().addShutdownHook(this.shutdownThread);
    }

    abstract Stream<R> valueStream();

    abstract long sizeOfFileOnDiskInBytes();

    public void close() {
        this.cleanup(false);
    }

    private void cleanup() {
        this.cleanup(true);
    }

    private void cleanup(boolean isTriggeredFromShutdownHook) {
        try {
            FileIOUtils.deleteDirectory(this.diskMapPathFile);
        } catch (IOException var3) {
            LOG.warn("Error while deleting the disk map directory=" + this.diskMapPath, var3);
        }

        if (!isTriggeredFromShutdownHook && this.shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(this.shutdownThread);
        }
    }
}
