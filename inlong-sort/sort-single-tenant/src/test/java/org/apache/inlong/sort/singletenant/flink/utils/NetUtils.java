/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.singletenant.flink.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class NetUtils {
    /**
     * Looking for an unused local port starting from given port.
     * If the given port is less than or equal to 0, then start from 1024.
     *
     * @param start the given start port
     * @return an unused local port, -1 if not found
     */
    public static int getUnusedLocalPort(int start) {
        for (int port = Math.max(start, 1024); port <= 65535; port++) {
            if (!isLocalPortInUse(port)) {
                return port;
            }
        }
        return -1;
    }

    /**
     * Check whether the given port is in use at local.
     *
     * @param port port to be checked
     * @return true if in use, otherwise false
     */
    public static boolean isLocalPortInUse(int port) {
        boolean flag = true;
        try {
            flag = isPortInUse("127.0.0.1", port);
        } catch (Exception ignored) {
            // ignored
        }
        return flag;
    }

    /**
     * Check whether the given port is in use online.
     *
     * @param host IP
     * @param port port
     * @return true if in use, otherwise false
     * @throws UnknownHostException thrown if the given IP is unknown
     */
    public static boolean isPortInUse(String host, int port) throws UnknownHostException {
        InetAddress theAddress = InetAddress.getByName(host);
        try {
            new Socket(theAddress, port);
            return true;
        } catch (IOException ignored) {
            // ignored
        }
        return false;
    }
}
