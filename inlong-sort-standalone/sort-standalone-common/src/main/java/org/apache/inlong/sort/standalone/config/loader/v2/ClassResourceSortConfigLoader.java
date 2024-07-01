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

package org.apache.inlong.sort.standalone.config.loader.v2;

import org.apache.inlong.common.pojo.sort.SortConfig;
import org.apache.inlong.sort.standalone.config.holder.v2.SortConfigType;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flume.Context;
import org.slf4j.Logger;

import java.nio.charset.Charset;

public class ClassResourceSortConfigLoader implements SortConfigLoader {

    public static final Logger LOG = InlongLoggerFactory.getLogger(ClassResourceSortConfigLoader.class);
    private Context context;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SortConfig load() {
        String fileName = SortConfigType.DEFAULT_FILE;
        try {
            if (context != null) {
                fileName = context.getString(SortConfigType.KEY_FILE, SortConfigType.DEFAULT_FILE);
            }
            String confString = IOUtils.toString(getClass().getClassLoader().getResource(fileName),
                    Charset.defaultCharset());
            int index = confString.indexOf('{');
            confString = confString.substring(index);
            SortConfig config = objectMapper.readValue(confString, SortConfig.class);
            return config;
        } catch (Exception e) {
            LOG.error("failed to load properties, file ={}", fileName, e);
        }
        return SortConfig.builder().build();
    }

    @Override
    public void configure(Context context) {
        this.context = context;
    }
}
