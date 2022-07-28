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

package org.apache.inlong.agent.plugin.metadata;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.plugin.SourceMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 *  meta information of kubernetes
 */
public class KubernetesMetadata implements SourceMeta {

    private static final Logger log = LoggerFactory.getLogger(KubernetesMetadata.class);

    private KubernetesClient kbClient;

    @Override
    public void init(JobProfile jobConf) {
        kbClient = getKubernetesClient(jobConf);
    }

    private KubernetesClient getKubernetesClient(JobProfile jobConf) {
        String ip = System.getProperty(CommonConstants.KUBERNETES_SERVICE_HOST);
        String port = System.getProperty(CommonConstants.KUBERNETES_SERVICE_PORT);
        if (Objects.isNull(ip) || Objects.isNull(port)) {
            log.warn("k8s env ip and port is null,can not connect k8s master");
            return null;
        }
        String maserUrl = CommonConstants.HTTPS.concat(ip).concat(CommonConstants.AGENT_COLON).concat(port);
        Config cofig = new ConfigBuilder().withMasterUrl(maserUrl).build();
        return new KubernetesClientBuilder().withConfig(cofig).build();
    }
    
    /**
     * get PODS of kubernetes information
     */
    public PodList getPods() {
        if (Objects.isNull(kbClient)) {
            return null;
        }
        MixedOperation<Pod, PodList, PodResource> pods = kbClient.pods();
        return pods.list();
    }

    /**
     *get pod metadata by namespace and pod name
     */
    public ObjectMeta getPodMetadata(String namespace, String podName) {
        List<ObjectMeta> objectMetas = kbClient.pods().list().getItems().stream().map(Pod::getMetadata)
                .filter(data -> data.getName().equalsIgnoreCase(podName) && data.getNamespace()
                        .equalsIgnoreCase(namespace)).collect(Collectors.toList());
        return CollectionUtils.isNotEmpty(objectMetas) ? objectMetas.get(0) : null;
    }

}
