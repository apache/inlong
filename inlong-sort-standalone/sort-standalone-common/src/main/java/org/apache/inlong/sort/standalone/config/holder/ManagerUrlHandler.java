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

package org.apache.inlong.sort.standalone.config.holder;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.slf4j.Logger;

import java.util.Optional;

/**
 * Manager address get handler.
 *
 * <p> Used to acquire the ip and port of manager, which sort sdk and sort-standalone request config from. </p>
 * <p> The default implementation is base on {@link CommonPropertiesHolder} to acquire properties. </p>
 */
public class ManagerUrlHandler implements Configurable {

    private static final Logger LOG = InlongLoggerFactory.getLogger(ManagerUrlHandler.class);
    private static final String KEY_MANAGER_URL_HANDLER_TYPE = "ManagerUrlHandler";
    private static final String KEY_SORT_CLUSTER_CONFIG_MANAGER_URL = "sortClusterConfig.managerUrl";
    private static final String KEY_SORT_SOURCE_CONFIG_MANAGER_URL = "sortSourceConfig.managerUrl";

    private static ManagerUrlHandler instance;
    private static String sortSourceConfigUrl;
    private static String sortClusterConfigUrl;

    public Context context;

    /**
     * Delete no argument constructor.
     */
    private ManagerUrlHandler() {

    }

    /**
     * Get URL where SortSdk request SortSourceConfig.
     *
     * @return URL to get SortSourceConfig.
     */
    public static String getSortSourceConfigUrl() {
        if (sortSourceConfigUrl != null) {
            return sortSourceConfigUrl;
        }
        sortSourceConfigUrl = get().context.getString(KEY_SORT_SOURCE_CONFIG_MANAGER_URL);
        if (StringUtils.isBlank(sortSourceConfigUrl)) {
            String warnMsg = "Get key" + KEY_SORT_SOURCE_CONFIG_MANAGER_URL
                    + " from CommonPropertiesHolder failed, it's a optional property use to specify "
                    + "the url where SortSdk request SortSourceConfig.";
            LOG.warn(warnMsg);
            sortSourceConfigUrl = warnMsg;
        }
        return sortSourceConfigUrl;
    }

    /**
     * Get URL where Sort-Standalone request SortClusterConfig.
     *
     * @return URL to get SortClusterConfig.
     */
    public static String getSortClusterConfigUrl() {
        if (sortClusterConfigUrl != null) {
            return sortClusterConfigUrl;
        }
        sortClusterConfigUrl = get().context.getString(KEY_SORT_CLUSTER_CONFIG_MANAGER_URL);
        if (StringUtils.isBlank(sortClusterConfigUrl)) {
            String warnMsg = "Get key" + KEY_SORT_CLUSTER_CONFIG_MANAGER_URL
                    + " from CommonPropertiesHolder failed, it's a optional property use to specify "
                    + "the url where Sort-Standalone request SortSourceConfig.";
            LOG.warn(warnMsg);
        }
        return sortClusterConfigUrl;
    }

    private static ManagerUrlHandler get() {
        if (instance != null) {
            return instance;
        }
        synchronized (ManagerUrlHandler.class) {
            String handlerType = CommonPropertiesHolder
                    .getString(KEY_MANAGER_URL_HANDLER_TYPE, ManagerUrlHandler.class.getName());
            LOG.info("Start to load ManagerUrlHandler, type is {}.", handlerType);
            try {
                Class<?> handlerClass = ClassUtils.getClass(handlerType);
                Object handlerObj = handlerClass.getDeclaredConstructor().newInstance();
                if (handlerObj instanceof ManagerUrlHandler) {
                    instance = (ManagerUrlHandler) handlerObj;
                }
            } catch (Throwable t) {
                LOG.error("Got exception when load ManagerAddrGetHandler, type is {}, err is {}",
                        handlerType, t.getMessage());
            }
            Optional.ofNullable(instance).ifPresent(inst -> inst.configure(new Context(CommonPropertiesHolder.get())));
        }
        return instance;
    }

    @Override
    public void configure(Context context) {
        Optional.ofNullable(context).ifPresent(c -> this.context = c);
    }

}
