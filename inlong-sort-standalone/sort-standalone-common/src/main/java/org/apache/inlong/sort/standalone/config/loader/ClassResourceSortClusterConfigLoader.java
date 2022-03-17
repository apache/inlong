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

package org.apache.inlong.sort.standalone.config.loader;

import java.io.UnsupportedEncodingException;

import org.apache.commons.io.IOUtils;
import org.apache.flume.Context;
import org.apache.inlong.common.pojo.sortstandalone.SortClusterConfig;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.slf4j.Logger;

import com.google.gson.Gson;

/**
 * 
 * ClassResourceCommonPropertiesLoader
 */
public class ClassResourceSortClusterConfigLoader implements SortClusterConfigLoader {

    public static final Logger LOG = InlongLoggerFactory.getLogger(ClassResourceSortClusterConfigLoader.class);
    public static final String FILENAME = "SortClusterConfig.conf";

    /**
     * load
     * 
     * @return
     */
    @Override
    public SortClusterConfig load() {
        try {
            String confString = IOUtils.toString(getClass().getClassLoader().getResource(FILENAME));
            Gson gson = new Gson();
            SortClusterConfig config = gson.fromJson(confString, SortClusterConfig.class);
            return config;
        } catch (UnsupportedEncodingException e) {
            LOG.error("fail to load properties, file ={}, and e= {}", FILENAME, e);
        } catch (Exception e) {
            LOG.error("fail to load properties, file ={}, and e= {}", FILENAME, e);
        }
        return SortClusterConfig.builder().build();
    }

    /**
     * configure
     * 
     * @param context
     */
    @Override
    public void configure(Context context) {
    }
}
