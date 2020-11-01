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

package org.apache.tubemq.server.master.web.handler;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WebApiMapper {
    // log printer
    private static final Logger logger =
            LoggerFactory.getLogger(WebApiMapper.class);
    // The query methods map
    public static final Map<String, WebApiRegInfo> WEB_QRY_METHOD_MAP =
            new HashMap<>();
    // The modify methods map
    public static final Map<String, WebApiRegInfo> WEB_MDY_METHOD_MAP =
            new HashMap<>();


    public static WebApiRegInfo getWebApiRegInfo(boolean isQryApi,
                                                 String webMethodName) {
        if (isQryApi) {
            return WEB_QRY_METHOD_MAP.get(webMethodName);
        }
        return WEB_MDY_METHOD_MAP.get(webMethodName);
    }

    public static void registerWebMethod(boolean isQryApi,
                                         String webMethodName,
                                         String clsMethodName,
                                         AbstractWebHandler webHandler) {
        Method[] methods = webHandler.getClass().getMethods();
        for (Method item : methods) {
            if (item.getName().equals(clsMethodName)) {
                if (isQryApi) {
                    WEB_QRY_METHOD_MAP.put(webMethodName,
                            new WebApiRegInfo(item, webHandler));
                } else {
                    WEB_MDY_METHOD_MAP.put(webMethodName,
                            new WebApiRegInfo(item, webHandler));
                }
                return;
            }
        }
        logger.error(new StringBuilder(512)
                .append("registerWebMethod failure, not found Method by clsMethodName")
                .append(clsMethodName).append(" in WebHandler class ")
                .append(webHandler.getClass().getName()).toString());
    }



    public static class WebApiRegInfo {
        public Method method;
        public AbstractWebHandler webHandler;

        public WebApiRegInfo(Method method,
                             AbstractWebHandler webHandler) {
            this.method = method;
            this.webHandler = webHandler;
        }
    }

}
