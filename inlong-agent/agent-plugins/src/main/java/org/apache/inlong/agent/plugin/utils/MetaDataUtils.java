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

import io.fabric8.kubernetes.api.model.Pod;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.plugin.SourceMeta;
import org.apache.inlong.agent.plugin.metadata.MetadataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.inlong.agent.constant.CommonConstants.COMMA;
import static org.apache.inlong.agent.constant.JobConstants.JOB_META_INFO_LIST;

/**
 * meta data utils for source and env
 */
public class MetaDataUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataUtils.class);

    public static List<String> joinPodMeta(List<String> lines, Pod pod) {
        return null;
    }

    /**
     * get source meta instance from job config
     *
     * @param jobProfile job config
     * @return source meta list
     */
    public static List<SourceMeta> getSourceMeta(JobProfile jobProfile) {
        if (!jobProfile.hasKey(JOB_META_INFO_LIST)) {
            return null;
        }
        List<SourceMeta> sourceMetas = new ArrayList<>();
        String[] metas = jobProfile.get(JOB_META_INFO_LIST).split(COMMA);
        for (String metaType : metas) {
            SourceMeta sourceMeta = MetadataFactory.getInstance(metaType);
            if (Objects.isNull(sourceMeta)) {
                LOGGER.warn("The meta type not support : {}", metaType);
                continue;
            }
            sourceMeta.init(jobProfile);
            sourceMetas.add(sourceMeta);
        }
        return sourceMetas;
    }

    /**
     * standard log for k8s
     * get pod_name,namespace,container_name,container_id
     */
    public static Map<String, String> getLogInfo(String fileName) {
        Map<String, String> podInf = new HashMap<>();
        if (!StringUtils.isNoneBlank(fileName) && fileName.contains(CommonConstants.DELIMITER_2)) {
            return podInf;
        }
        //file name example: /var/log/containers/<pod_name>_<namespace>_<container_name>-<continer_id>.log
        String[] str = fileName.split(CommonConstants.DELIMITER_2);
        podInf.put(CommonConstants.POD_NAME, str[0]);
        podInf.put(CommonConstants.NAMESPACE, str[1]);
        String[] containerInfo = str[2].split(CommonConstants.DELIMITER_1);
        podInf.put(CommonConstants.CONTAINER_NAME, containerInfo[0]);
        podInf.put(CommonConstants.CONTAINER_ID, containerInfo[1]);
        return podInf;
    }
}
