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

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flume.Context;
import org.apache.inlong.dataproxy.config.pojo.IdTopicConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * TestContextCacheClusterConfigLoader
 */
public class TestContextIdTopicConfigLoader {

    public static final Logger LOG = LoggerFactory.getLogger(TestContextIdTopicConfigLoader.class);
    private static Context context;
    private static Context sinkContext;

    /**
     * setup
     */
    @BeforeClass
    public static void setup() {
        Map<String, String> result = new ConcurrentHashMap<>();
        try (InputStream inStream = TestContextIdTopicConfigLoader.class.getClassLoader().getResource("flume.conf")
                .openStream()) {
            Properties props = new Properties();
            props.load(inStream);
            for (Map.Entry<Object, Object> entry : props.entrySet()) {
                result.put((String) entry.getKey(), (String) entry.getValue());
            }
            context = new Context(result);
            sinkContext = new Context(context.getSubProperties("proxy_inlong5th_sz.sinks.pulsar-sink-more1."));
        } catch (UnsupportedEncodingException e) {
            LOG.error("fail to load properties, file ={}, and e= {}", "flume.conf", e);
        } catch (Exception e) {
            LOG.error("fail to load properties, file ={}, and e= {}", "flume.conf", e);
        }
    }

    /**
     * testResult
     * 
     * @throws Exception
     */
    @Test
    public void testResult() throws Exception {
        ContextIdTopicConfigLoader loader = new ContextIdTopicConfigLoader();
        loader.configure(sinkContext);
        List<IdTopicConfig> configList = loader.load();
        assertEquals(2, configList.size());

        for (IdTopicConfig config : configList) {
            if ("03a00000026".equals(config.getInlongGroupId())) {
                assertEquals("pulsar-9xn9wp35pbxb/test/topic1", config.getTopicName());
            }
        }
    }
}
