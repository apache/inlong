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
 *
 */

package org.apache.inlong.agent.plugin.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.constant.CommonConstants;

import java.util.HashMap;
import java.util.Map;

import static org.apache.inlong.agent.constant.KubernetesConstants.CONTAINER_ID;
import static org.apache.inlong.agent.constant.KubernetesConstants.CONTAINER_NAME;
import static org.apache.inlong.agent.constant.KubernetesConstants.NAMESPACE;
import static org.apache.inlong.agent.constant.KubernetesConstants.POD_NAME;

/**
 * metadata utils
 */
public class MetaDataUtils {

    /**
     * standard log for k8s
     * get pod_name,namespace,container_name,container_id
     */
    public static Map<String, String> getLogInfo(String fileName) {
        Map<String, String> podInf = new HashMap<>();
        if (!StringUtils.isNoneBlank(fileName) && fileName.contains(CommonConstants.DELIMITER_UNDERLINE)) {
            return podInf;
        }
        //file name example: /var/log/containers/<pod_name>_<namespace>_<container_name>-<continer_id>.log
        String[] str = fileName.split(CommonConstants.DELIMITER_UNDERLINE);
        podInf.put(POD_NAME, str[0]);
        podInf.put(NAMESPACE, str[1]);
        String[] containerInfo = str[2].split(CommonConstants.DELIMITER_HYPHEN);
        podInf.put(CONTAINER_NAME, containerInfo[0]);
        podInf.put(CONTAINER_ID, containerInfo[1]);
        return podInf;
    }

    public static String concatString(String str1, String str2) {
        if (!StringUtils.isNoneBlank(str2)) {
            return str1;
        }
        return str1.concat("\n").concat(str2);
    }
}
