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

package org.apache.inlong.manager.service.schedule;

import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.workflow.processor.OfflineJobOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OfflineJobOperatorFactory {

    private static final Logger log = LoggerFactory.getLogger(OfflineJobOperatorFactory.class);
    public static final String DEFAULT_OPERATOR_CLASS_NAME =
            "org.apache.inlong.manager.plugin.offline.FlinkOfflineJobOperator";

    public static OfflineJobOperator getOfflineJobOperator() {
        return getOfflineJobOperator(DEFAULT_OPERATOR_CLASS_NAME);
    }

    public static OfflineJobOperator getOfflineJobOperator(String operatorClassName) {
        return getOfflineJobOperator(operatorClassName, Thread.currentThread().getContextClassLoader());
    }

    public static OfflineJobOperator getOfflineJobOperator(String operatorClassName, ClassLoader classLoader) {
        try {
            Class<?> operatorClass = classLoader.loadClass(operatorClassName);
            Object operator = operatorClass.getDeclaredConstructor().newInstance();
            return (OfflineJobOperator) operator;
        } catch (Throwable e) {
            log.error("Failed to get offline job operator: ", e);
            throw new BusinessException("Failed to get offline job operator: " + e.getMessage());
        }
    }

}
