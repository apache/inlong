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

package org.apache.inlong.dataproxy.config.loader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * FileCommonPropertiesLoader
 */
public class ClassResourceCommonPropertiesLoader implements CommonPropertiesLoader {

    public static final Logger LOG = LoggerFactory.getLogger(ClassResourceCommonPropertiesLoader.class);

    /**
     * load
     * 
     * @return
     */
    @Override
    public Map<String, String> load() {
        return this.loadProperties("common.properties");
    }

    /**
     * loadProperties
     * 
     * @param  fileName
     * @return
     */
    protected Map<String, String> loadProperties(String fileName) {
        Map<String, String> result = new ConcurrentHashMap<>();
        InputStream inStream = null;
        try {
            URL url = getClass().getClassLoader().getResource(fileName);
            inStream = url != null ? url.openStream() : null;

            if (inStream == null) {
                LOG.error("InputStream {} is null!", fileName);
            }
            Properties props = new Properties();
            props.load(inStream);
            for (Map.Entry<Object, Object> entry : props.entrySet()) {
                result.put((String) entry.getKey(), (String) entry.getValue());
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("fail to load properties, file ={}, and e= {}", fileName, e);
        } catch (Exception e) {
            LOG.error("fail to load properties, file ={}, and e= {}", fileName, e);
        } finally {
            if (null != inStream) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    LOG.error("fail to loadTopics, inStream.close ,and e= {}", fileName, e);
                }
            }
        }
        return result;
    }
}
