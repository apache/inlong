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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class provides a disk spillable only kv buffer implementation.
 * All of the data is stored using the RocksDB implementation.
 */
public final class RocksDBKVBuffer<T, R> implements Closeable, KVBuffer<T, R>, Serializable {

    // ColumnFamily allows partitioning data within RockDB, which allows
    // independent configuration and faster deletes across partitions
    // https://github.com/facebook/rocksdb/wiki/Column-Families
    // For this use case, we use a single static column family/ partition
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBKVBuffer.class);
    private static final long serialVersionUID = 1L;
    private static final String ROCKSDB_COL_FAMILY = "rocksdb-diskmap";

    private transient RocksDBDAO<T, R> rocksDb;
    private transient boolean closed = false;
    private transient Thread shutdownThread = null;
    protected String diskMapPath;
    protected final TypeSerializer<T> keySerializer;
    protected final TypeSerializer<R> valueSerializer;
    protected final String rocksDbStoragePath;

    public RocksDBKVBuffer(
            TypeSerializer<T> keySerializer,
            TypeSerializer<R> valueSerializer,
            String rocksDbStoragePath) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.rocksDbStoragePath = rocksDbStoragePath;
    }

    @Override
    public R put(T key, R value) {
        checkClosed();
        lazyGetRocksDb().put(ROCKSDB_COL_FAMILY, key, value);
        return value;
    }

    @Override
    public R remove(T key) {
        checkClosed();
        R value = get(key);
        if (value != null) {
            lazyGetRocksDb().delete(ROCKSDB_COL_FAMILY, (T) key);
        }
        return value;
    }

    @Override
    public R get(T key) {
        checkClosed();
        return lazyGetRocksDb().get(ROCKSDB_COL_FAMILY, key);
    }

    @Override
    public Stream<Tuple2<T, R>> scan(byte[] keyPrefix) {
        checkClosed();
        return lazyGetRocksDb().prefixSearch(ROCKSDB_COL_FAMILY, keyPrefix);
    }

    @Override
    public Stream<R> valueStream() {
        checkClosed();
        Iterator<R> iterator = lazyGetRocksDb().iterator(ROCKSDB_COL_FAMILY);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    @Override
    public void clear() {
        checkClosed();
        lazyGetRocksDb().dropColumnFamily(ROCKSDB_COL_FAMILY);
        lazyGetRocksDb().addColumnFamily(ROCKSDB_COL_FAMILY);
    }

    @Override
    public void close() {
        if (null != rocksDb) {
            rocksDb.close();
        }
        rocksDb = null;
        closed = true;
        this.cleanup(false);
    }

    private void checkClosed() {
        Preconditions.checkArgument(!closed, "Could not operate a close RocksDB KV Buffer");
    }

    private void cleanup() {
        this.cleanup(true);
    }

    private void addShutDownHook() {
        this.shutdownThread = new Thread(this::cleanup);
        Runtime.getRuntime().addShutdownHook(this.shutdownThread);
    }

    private void cleanup(boolean isTriggeredFromShutdownHook) {
        try {
            FileIOUtils.deleteDirectory(new File(diskMapPath));
        } catch (IOException var3) {
            LOG.warn("Error while deleting the disk map directory=" + this.diskMapPath, var3);
        }

        if (!isTriggeredFromShutdownHook && this.shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(this.shutdownThread);
        }
    }

    private RocksDBDAO<T, R> lazyGetRocksDb() {
        if (null == rocksDb) {
            synchronized (this) {
                if (null == rocksDb) {
                    diskMapPath = String.format("%s%s%s-%s",
                            rocksDbStoragePath, File.separator, "rocksdb", UUID.randomUUID());
                    LOG.info("Open rocksdb dir in {}", diskMapPath);
                    try {
                        File diskMapPathFile = new File(diskMapPath);
                        FileIOUtils.deleteDirectory(diskMapPathFile);
                        FileIOUtils.mkdir(diskMapPathFile);
                        diskMapPathFile.deleteOnExit();
                    } catch (IOException e) {
                        LOG.warn("Open rocksdb dir occur error", e);
                        throw new RuntimeException(e);
                    }
                    rocksDb = new RocksDBDAO<>(
                            ROCKSDB_COL_FAMILY, diskMapPath, keySerializer, valueSerializer);
                    rocksDb.addColumnFamily(ROCKSDB_COL_FAMILY);
                    this.addShutDownHook();
                }
            }
        }
        return rocksDb;
    }
}
