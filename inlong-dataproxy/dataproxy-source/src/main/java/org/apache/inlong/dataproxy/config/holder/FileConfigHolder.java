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

package org.apache.inlong.dataproxy.config.holder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.IOUtils;
import org.apache.inlong.dataproxy.config.ConfigHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * save to list
 */
public class FileConfigHolder extends ConfigHolder {

    private static final Logger LOG = LoggerFactory.getLogger(FileConfigHolder.class);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private List<String> holder;

    public FileConfigHolder(String fileName) {
        super(fileName);
        holder = new ArrayList<String>();
    }

    @Override
    public void loadFromFileToHolder() {
        readWriteLock.readLock().lock();
        try {
            List<String> tmpHolder = loadFile();
            LOG.info(getFileName() + " load content {}", tmpHolder);
            holder = tmpHolder;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * deep copy holder
     *
     * @return
     */
    public List<String> forkHolder() {
        List<String> tmpHolder = new ArrayList<String>();
        if (holder != null) {
            tmpHolder.addAll(holder);
        }
        return tmpHolder;
    }

    private List<String> loadFile() {
        ArrayList<String> arrayList = new ArrayList<String>();
        FileReader reader = null;
        BufferedReader br = null;
        try {
            reader = new FileReader(getFilePath());
            br = new BufferedReader(reader);
            String line;
            while ((line = br.readLine()) != null) {
                arrayList.add(line);
            }
        } catch (Exception e) {
            LOG.error("fail to load file, file ={}, and e= {}", getFilePath(), e);
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(br);
        }
        return arrayList;
    }

    public List<String> getHolder() {
        return holder;
    }
}
