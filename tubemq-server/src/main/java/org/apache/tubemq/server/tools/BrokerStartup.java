/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.tools;

import org.apache.tubemq.server.broker.BrokerConfig;
import org.apache.tubemq.server.broker.TubeBroker;
import org.apache.tubemq.server.common.utils.ProcessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerStartup {

    private static final Logger logger =
            LoggerFactory.getLogger(BrokerStartup.class);

    public static void main(final String[] args) throws Exception {
        // get configure file path
        ProcessResult result = new ProcessResult();
        if (!CliUtils.getConfigFilePath(args, result)) {
            System.err.println(result.errInfo);
            System.exit(1);
        }
        String configFilePath = (String) result.retData1;
        // read configure file
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.loadFromFile(configFilePath);
        logger.info("[BrokerStartup] Broker config is: " + brokerConfig);
        // start broker instance
        final TubeBroker server = new TubeBroker(brokerConfig);
        server.start();
    }
}
