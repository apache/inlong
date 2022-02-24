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

import java.util.Map;

import org.apache.inlong.dataproxy.config.RemoteConfigManager;
import org.apache.inlong.common.metric.MetricListener;
import org.junit.Test;

/**
 * 
 * TestClassResourceCommonPropertiesLoader
 */
public class TestClassResourceCommonPropertiesLoader {

    /**
     * testResult
     * 
     * @throws Exception
     */
    @Test
    public void testResult() throws Exception {
        // increase source
        ClassResourceCommonPropertiesLoader loader = new ClassResourceCommonPropertiesLoader();
        Map<String, String> props = loader.load();
        assertEquals("proxy_inlong5th_sz", props.get(RemoteConfigManager.KEY_PROXY_CLUSTER_NAME));
        assertEquals("DataProxy", props.get(MetricListener.KEY_METRIC_DOMAINS));
    }
}
