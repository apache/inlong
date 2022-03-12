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

package org.apache.inlong.manager.service.utils;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

/**
 * SpringContextUtils
 */
public class SpringContextUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SpringContextUtils.class);

    private static Properties props = new Properties();
    private static ConfigurableApplicationContext applicationContext;

    /**
     * getApplicationContext
     * 
     * @return
     */
    public static ConfigurableApplicationContext getApplicationContext() {
        return applicationContext;
    }

    /**
     * setApplicationContext
     * 
     * @param applicationContext
     */
    public static void setApplicationContext(ConfigurableApplicationContext applicationContext) {
        SpringContextUtils.applicationContext = applicationContext;
        try {
            Resource resource = new ClassPathResource("/bean.properties");
            props = PropertiesLoaderUtils.loadProperties(resource);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * getBean
     * 
     * @param  beanName
     * @param  defaultClass
     * @return Object
     */
    public static Object getBean(String beanName, String defaultClass) {
        Object obj = null;
        try {
            obj = applicationContext.getBean(beanName);
            if (obj != null) {
                return obj;
            }
        } catch (Exception e) {
            LOG.error("Can not getBean:{},error:{}", beanName, e.getMessage());
        }
        // get bean class name
        String beanClassName = props.getProperty(beanName, defaultClass);
        try {
            registerBean(beanName, beanClassName);
            obj = applicationContext.getBean(beanName);
            if (obj != null) {
                return obj;
            }
        } catch (Exception e) {
            LOG.error("Can not registerBean:{},beanClassName:{},defaultClass:{},error:{}", beanName, beanClassName,
                    defaultClass, e);
        }
        return null;
    }

    /**
     * getBean
     * 
     * @param  requiredType
     * @return
     */
    public static Object getBean(Class<?> requiredType) {
        return applicationContext.getBean(requiredType);
    }

    /**
     * registerBean
     * 
     * @param beanName
     * @param beanClassName
     */
    private static void registerBean(String beanName, String beanClassName) {
        DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getBeanFactory();
        BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.genericBeanDefinition(beanClassName);
        beanFactory.registerBeanDefinition(beanName, beanBuilder.getRawBeanDefinition());
    }
}
