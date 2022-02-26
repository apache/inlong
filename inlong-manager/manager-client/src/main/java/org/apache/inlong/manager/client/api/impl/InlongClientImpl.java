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

package org.apache.inlong.manager.client.api.impl;

import com.github.pagehelper.PageInfo;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupConf;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;

@Slf4j
public class InlongClientImpl implements InlongClient {

    @Getter
    private ClientConfiguration configuration;

    private static final String URL_SPLITTER = ",";

    private static final String HOST_SPLITTER = ":";

    public InlongClientImpl(String serviceUrl, ClientConfiguration configuration) {
        Map<String, String> hostPorts = Splitter.on(URL_SPLITTER).withKeyValueSeparator(HOST_SPLITTER)
                .split(serviceUrl);
        if (MapUtils.isEmpty(hostPorts)) {
            throw new IllegalArgumentException(String.format("Unsupported serviceUrl : %s", serviceUrl));
        }
        configuration.setServiceUrl(serviceUrl);
        boolean isConnective = false;
        for (Map.Entry<String, String> hostPort : hostPorts.entrySet()) {
            String host = hostPort.getKey();
            int port = Integer.valueOf(hostPort.getValue());
            if (checkConnectivity(host, port, configuration.getReadTimeout())) {
                configuration.setBindHost(host);
                configuration.setBindPort(port);
                isConnective = true;
                break;
            }
        }
        if (!isConnective) {
            throw new RuntimeException(String.format("%s is not connective", serviceUrl));
        }
        this.configuration = configuration;
    }

    @Override
    public InlongGroup createGroup(InlongGroupConf groupConf) throws Exception {
        return new InlongGroupImpl(groupConf, this);
    }

    @Override
    public PageInfo<InlongGroupListResponse> listGroup(String keyword, int status,
            int pageNum, int pageSize) throws Exception {
        InnerInlongManagerClient managerClient = new InnerInlongManagerClient(this);
        return managerClient.listGroupInfo(keyword, status, pageNum, pageSize);
    }

    @Override
    public InlongGroupRequest getGroup(String groupId) throws Exception {
        InnerInlongManagerClient managerClient = new InnerInlongManagerClient(this);
        return managerClient.getGroupInfo(groupId);
    }

    private boolean checkConnectivity(String host, int port, int connectTimeout) {
        InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        Socket socket = new Socket();
        try {
            socket.connect(socketAddress, connectTimeout * 1000);
            return socket.isConnected();
        } catch (IOException e) {
            log.error(String.format("%s:%s connected failed with err msg:%s", host, port, e.getMessage()));
            return false;
        }
    }
}
