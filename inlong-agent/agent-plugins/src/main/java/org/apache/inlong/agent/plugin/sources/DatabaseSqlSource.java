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

package org.apache.inlong.agent.plugin.sources;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;
import org.apache.inlong.agent.plugin.sources.file.extend.DefaultExtendedHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Make database as Source
 */
public class DatabaseSqlSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseSqlSource.class);

    private static AtomicLong metricsIndex = new AtomicLong(0);

    public DatabaseSqlSource() {
    }

    @Override
    protected String getThreadName() {
        return null;
    }

    @Override
    protected void initExtendClass() {
        extendClass = DefaultExtendedHandler.class.getCanonicalName();
    }

    @Override
    protected void initSource(InstanceProfile profile) {

    }

    @Override
    protected void printCurrentState() {

    }

    @Override
    protected boolean doPrepareToRead() {
        return false;
    }

    @Override
    protected List<SourceData> readFromSource() {
        return null;
    }

    @Override
    public Message read() {
        return null;
    }

    @Override
    protected boolean isRunnable() {
        return runnable;
    }

    @Override
    protected void releaseSource() {

    }

    @Override
    public boolean sourceFinish() {
        return false;
    }

    @Override
    public boolean sourceExist() {
        return false;
    }
}
