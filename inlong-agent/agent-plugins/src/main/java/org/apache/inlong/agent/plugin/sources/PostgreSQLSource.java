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
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.file.Reader;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;
import org.apache.inlong.agent.plugin.sources.reader.PostgreSQLReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL source, split PostgreSQL source job into multi readers
 */
public class PostgreSQLSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLSource.class);

    public PostgreSQLSource() {

    }

    @Override
    public List<Reader> split(TaskProfile conf) {
        PostgreSQLReader postgreSQLReader = new PostgreSQLReader();
        List<Reader> readerList = new ArrayList<>();
        readerList.add(postgreSQLReader);
        sourceMetric.sourceSuccessCount.incrementAndGet();
        return readerList;
    }

    @Override
    protected String getThreadName() {
        return null;
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
