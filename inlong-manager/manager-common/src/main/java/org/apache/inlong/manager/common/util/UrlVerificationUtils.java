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

package org.apache.inlong.manager.common.util;

import org.apache.inlong.manager.common.consts.InlongConstants;

public class UrlVerificationUtils {

    /**
     * Extracts the hostname and validates the port from a JDBC URL with the specified prefix.
     *
     * @param fullUrl The full JDBC URL to extract the hostname and port from
     * @param prefix  The expected prefix of the JDBC URL
     * @throws Exception If the URL format is invalid or the port is invalid
     */
    public static void extractHostAndValidatePortFromJdbcUrl(String fullUrl, String prefix) throws Exception {
        if (!fullUrl.startsWith(prefix)) {
            throw new Exception("Invalid JDBC URL, it should start with " + prefix);
        }
        // Extract the host and port part after the prefix
        String hostPortPart = fullUrl.substring(prefix.length());
        String[] hostPortParts = hostPortPart.split(InlongConstants.SLASH);

        if (hostPortParts.length < 1) {
            throw new Exception("Invalid JDBC URL format");
        }
        String hostPort = hostPortParts[0];
        String[] hostPortSplit = hostPort.split(InlongConstants.COLON);
        if (hostPortSplit.length != 2) {
            throw new Exception("Invalid host:port format in JDBC URL");
        }

        String portStr = hostPortSplit[1];
        try {
            int portNumber = Integer.parseInt(portStr);
            if (portNumber < 1 || portNumber > 65535) {
                throw new Exception("Invalid port number in JDBC URL");
            }
        } catch (NumberFormatException e) {
            throw new Exception("Invalid port number format in JDBC URL");
        }
    }
}
