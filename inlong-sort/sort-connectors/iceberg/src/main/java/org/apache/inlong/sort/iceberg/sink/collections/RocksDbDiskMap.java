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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class provides a disk spillable only map implementation.
 * All of the data is stored using the RocksDB implementation.
 */
public final class RocksDbDiskMap<T, R> extends DiskMap<T, R> {

    private static final Logger LOG = LogManager.getLogger(RocksDbDiskMap.class);

    // ColumnFamily allows partitioning data within RockDB, which allows
    // independent configuration and faster deletes across partitions
    // https://github.com/facebook/rocksdb/wiki/Column-Families
    // For this use case, we use a single static column family/ partition
    //
    private static final String ROCKSDB_COL_FAMILY = "rocksdb-diskmap";

    // 本类其实是SpillBuffer，而不是一个map
    // 因为数据量太大，如果要满足查询size，isEmpty等接口需要保存key，仍然需要存储在磁盘中，放在内存中会爆炸
    private RocksDBDAO<T, R> rocksDb;

    public RocksDbDiskMap(MapStateDescriptor<T, R> descriptor, String rocksDbStoragePath) throws IOException {
        super(descriptor, rocksDbStoragePath, "rocksdb");
    }

    @Override
    public int size() {
        throw new RuntimeException("unable to compare keys in map");
    }

    @Override
    public boolean isEmpty() {
        throw new RuntimeException("unable to compare keys in map");
    }

    @Override
    public boolean containsKey(Object key) {
        throw new RuntimeException("unable to compare keys in map");
    }

    @Override
    public boolean containsValue(Object value) {
        throw new RuntimeException("unable to compare values in map");
    }

    @Override
    public R get(Object key) {
        if (!containsKey(key)) {
            return null;
        }
        return getRocksDb().get(ROCKSDB_COL_FAMILY, (T) key);
    }

    /**
     * Scan as key prefix, it will return all key prefix data
     * @param keyPrefix
     * @return
     */
    public Stream<Tuple2<T, R>> scan(byte[] keyPrefix) {
        return getRocksDb().prefixSearch(ROCKSDB_COL_FAMILY, keyPrefix);
    }

    @Override
    public R put(T key, R value) {
        getRocksDb().put(ROCKSDB_COL_FAMILY, key, value);
        return value;
    }

    @Override
    public R remove(Object key) {
        R value = get(key);
        if (value != null) {
            getRocksDb().delete(ROCKSDB_COL_FAMILY, (T) key);
        }
        return value;
    }

    @Override
    public void putAll(Map<? extends T, ? extends R> keyValues) {
        getRocksDb().writeBatch(batch -> keyValues
                .forEach((key, value) -> getRocksDb().putInBatch(batch, ROCKSDB_COL_FAMILY, key, value)));
    }

    @Override
    public void clear() {
        close();
    }

    @Override
    public Set<T> keySet() {
        throw new RuntimeException("unable to compare keys in map");
    }

    @Override
    public Collection<R> values() {
        throw new RuntimeException("Unsupported Operation Exception");
    }

    @Override
    public Set<Entry<T, R>> entrySet() {
        throw new RuntimeException("unable to compare keys in map");
    }

    /**
     * Custom iterator to iterate over values written to disk.
     */
    @Override
    public Iterator<R> iterator() {
        return getRocksDb().iterator(ROCKSDB_COL_FAMILY);
    }

    @Override
    public Stream<R> valueStream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), 0), false);
    }

    @Override
    public long sizeOfFileOnDiskInBytes() {
        return getRocksDb().getTotalBytesWritten();
    }

    @Override
    public void close() {
        if (null != rocksDb) {
            rocksDb.close();
        }
        rocksDb = null;
        super.close();
    }

    private RocksDBDAO<T, R> getRocksDb() {
        if (null == rocksDb) {
            synchronized (this) {
                if (null == rocksDb) {

                    rocksDb = new RocksDBDAO<>(
                            ROCKSDB_COL_FAMILY, diskMapPath,
                            descriptor.getKeySerializer(), descriptor.getValueSerializer());
                    rocksDb.addColumnFamily(ROCKSDB_COL_FAMILY);
                }
            }
        }
        return rocksDb;
    }
}
