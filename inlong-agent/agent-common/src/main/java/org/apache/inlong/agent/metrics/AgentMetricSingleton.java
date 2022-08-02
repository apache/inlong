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

package org.apache.inlong.agent.metrics;

import org.apache.commons.lang3.ClassUtils;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.inlong.agent.constant.AgentConstants.AGENT_METRIC_LISTENER_CLASS;
import static org.apache.inlong.agent.constant.AgentConstants.AGENT_METRIC_LISTENER_CLASS_DEFAULT;

/**
 * metric singleton
 */
public class AgentMetricSingleton {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentMetricSingleton.class);
    private static volatile AgentMetricBaseListener agentMetricBaseHandler;

    private AgentMetricSingleton() {
    }

    public static AgentMetricBaseListener getAgentMetricHandler() {
        if (agentMetricBaseHandler == null) {
            synchronized (AgentJmxMetricListener.class) {
                if (agentMetricBaseHandler == null) {
                    agentMetricBaseHandler = getAgentMetricByConf();
                    agentMetricBaseHandler.init();
                    if (agentMetricBaseHandler != null) {
                        LOGGER.info("The metric class {} was initialized successfully.",
                                agentMetricBaseHandler.getClass());
                    }
                }
            }
        }
        return agentMetricBaseHandler;
    }

    private static AgentMetricBaseListener getAgentMetricByConf() {
        AgentConfiguration conf = AgentConfiguration.getAgentConf();
        try {
            Class<?> handlerClass = ClassUtils
                    .getClass(conf.get(AGENT_METRIC_LISTENER_CLASS, AGENT_METRIC_LISTENER_CLASS_DEFAULT));
            Object handlerObject = handlerClass.getDeclaredConstructor().newInstance();
            return (AgentMetricBaseListener) handlerObject;
        } catch (Exception ex) {
            LOGGER.error("cannot find AgentMetricBaseHandler, {}", ex.getMessage());
        }
        return null;
    }
}
