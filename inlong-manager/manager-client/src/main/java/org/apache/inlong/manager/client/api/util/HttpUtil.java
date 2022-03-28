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

package org.apache.inlong.manager.client.api.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HttpUtil {

    public static boolean checkConnectivity(String host, int port, int connectTimeout, TimeUnit timeUnit) {
        InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        Socket socket = new Socket();
        try {
            socket.connect(socketAddress, (int) timeUnit.toMillis(connectTimeout));
            return socket.isConnected();
        } catch (IOException e) {
            log.error(String.format("%s:%s connected failed with err msg:%s", host, port, e.getMessage()));
            return false;
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                log.warn("close connection from {}:{} failed", host, port, e);
            }
        }
    }

}
