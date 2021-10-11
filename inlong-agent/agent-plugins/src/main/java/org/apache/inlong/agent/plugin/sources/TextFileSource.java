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

import static org.apache.inlong.agent.constants.CommonConstants.POSITION_SUFFIX;
import static org.apache.inlong.agent.constants.JobConstants.DEFAULT_JOB_LINE_FILTER;
import static org.apache.inlong.agent.constants.JobConstants.DEFAULT_JOB_READ_WAIT_TIMEOUT;
import static org.apache.inlong.agent.constants.JobConstants.JOB_LINE_FILTER_PATTERN;
import static org.apache.inlong.agent.constants.JobConstants.JOB_READ_WAIT_TIMEOUT;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.plugin.Reader;
import org.apache.inlong.agent.plugin.Source;
import org.apache.inlong.agent.plugin.sources.reader.TextFileReader;
import org.apache.inlong.agent.plugin.utils.PluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read text files
 */
public class TextFileSource implements Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(TextFileSource.class);

    // path + suffix
    public static final String MD5_SUFFIX = ".md5";

    @Override
    public List<Reader> split(JobProfile jobConf) {
        Collection<File> allFiles = PluginUtils.findSuitFiles(jobConf);
        List<Reader> result = new ArrayList<>();
        String filterPattern = jobConf.get(JOB_LINE_FILTER_PATTERN, DEFAULT_JOB_LINE_FILTER);
        for (File file : allFiles) {
            int seekPosition = jobConf.getInt(file.getAbsolutePath() + POSITION_SUFFIX, 0);
            LOGGER.info("read from history position {} with job profile {}", seekPosition, jobConf.getInstanceId());
            String md5 = jobConf.get(file.getAbsolutePath() + MD5_SUFFIX, "");
            TextFileReader textFileReader = new TextFileReader(file, seekPosition);
            long waitTimeout = jobConf.getLong(JOB_READ_WAIT_TIMEOUT, DEFAULT_JOB_READ_WAIT_TIMEOUT);
            textFileReader.setWaitMillisecs(waitTimeout);
            addValidator(filterPattern, textFileReader);
            result.add(textFileReader);
        }
        return result;
    }

    private void addValidator(String filterPattern, TextFileReader textFileReader) {
        textFileReader.addPatternValidator(filterPattern);
    }
}
