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

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

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

        String host = hostPortSplit[0];
        String portStr = hostPortSplit[1];
        try {
            int portNumber = Integer.parseInt(portStr);
            if (portNumber < 1 || portNumber > 65535) {
                throw new Exception("Invalid port number in JDBC URL");
            }
        } catch (NumberFormatException e) {
            throw new Exception("Invalid port number format in JDBC URL");
        }

        // Validate host is not a private/internal address
        validateHostNotInternal(host);
    }

    /**
     * Validates that the given URL does not target internal/private network addresses.
     * This method prevents SSRF attacks by blocking requests to:
     * - Loopback addresses (127.0.0.0/8, ::1)
     * - Link-local addresses (169.254.0.0/16, fe80::/10)
     * - Private network addresses (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
     * - Any address (0.0.0.0)
     *
     * @param url The URL string to validate (can be a full URL or host:port)
     * @throws Exception If the URL targets an internal address or cannot be resolved
     */
    public static void validateUrlNotInternal(String url) throws Exception {
        if (url == null || url.trim().isEmpty()) {
            throw new Exception("URL cannot be null or empty");
        }

        String host;
        try {
            // Try parsing as a full URI first
            if (url.contains("://")) {
                URI uri = new URI(url);
                host = uri.getHost();
            } else {
                // Treat as host:port
                String[] parts = url.split(InlongConstants.COLON);
                host = parts[0];
            }
        } catch (Exception e) {
            throw new Exception("Invalid URL format: " + url);
        }

        if (host == null || host.trim().isEmpty()) {
            throw new Exception("Unable to extract host from URL: " + url);
        }

        validateHostNotInternal(host);
    }

    /**
     * Validates that a hostname does not resolve to an internal/private IP address.
     *
     * @param host The hostname or IP address to validate
     * @throws Exception If the host resolves to an internal address
     */
    public static void validateHostNotInternal(String host) throws Exception {
        if (host == null || host.trim().isEmpty()) {
            throw new Exception("Host cannot be null or empty");
        }

        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            throw new Exception("Unable to resolve host: " + host);
        }

        for (InetAddress address : addresses) {
            if (isInternalAddress(address)) {
                throw new Exception(
                        "Connection to internal/private network addresses is not allowed: " + host);
            }
        }
    }

    /**
     * Checks if an InetAddress is an internal/private network address.
     *
     * @param address The address to check
     * @return true if the address is internal/private
     */
    private static boolean isInternalAddress(InetAddress address) {
        return address.isLoopbackAddress()
                || address.isLinkLocalAddress()
                || address.isSiteLocalAddress()
                || address.isAnyLocalAddress()
                || address.isMulticastAddress()
                || isCloudMetadataAddress(address);
    }

    /**
     * Checks if the address is a well-known cloud metadata service address.
     * AWS/GCP/Azure metadata service: 169.254.169.254
     *
     * @param address The address to check
     * @return true if the address is a cloud metadata address
     */
    private static boolean isCloudMetadataAddress(InetAddress address) {
        byte[] addr = address.getAddress();
        // Check for 169.254.169.254 (cloud metadata endpoint)
        if (addr.length == 4) {
            return (addr[0] & 0xFF) == 169
                    && (addr[1] & 0xFF) == 254
                    && (addr[2] & 0xFF) == 169
                    && (addr[3] & 0xFF) == 254;
        }
        return false;
    }
}
