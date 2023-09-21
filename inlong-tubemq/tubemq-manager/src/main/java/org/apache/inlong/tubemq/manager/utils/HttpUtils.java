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

package org.apache.inlong.tubemq.manager.utils;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class HttpUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtils.class);
    private static final int MAX_URL_LENGTH = 2048;
    private static final Pattern URL_PATTERN = Pattern.compile("^https?://[a-zA-Z0-9.-]+(/.*)?$");
    private static final Pattern PATH_TRAVERSAL_PATTERN = Pattern.compile("/\\.\\./|\\\\\\.\\./");

    public static String sendHttpGetRequest(String url) throws IOException {
        // Log audit: Record the received URL
        LOGGER.info("Received URL for HTTP GET request: {}", url);

        // Filter out special characters from the input URL
        url = filterSpecialCharacters(url);

        // Validate the input URL format
        if (!isValidURL(url)) {
            throw new SecurityException("Invalid URL format.");
        }

        // Check for path traversal attack
        if (containsPathTraversal(url)) {
            throw new SecurityException("Path traversal attempt detected.");
        }

        // Check the length of the input URL
        if (url.length() > MAX_URL_LENGTH) {
            throw new SecurityException("URL length exceeds the maximum allowed.");
        }

        // Create an HttpClient instance
        HttpClient httpClient = HttpClients.createDefault();

        // Create an HttpGet request
        HttpGet httpGet = new HttpGet(url);

        // Setting the connection timeout and read timeout
        int connectionTimeout = 10000;
        int readTimeout = 20000;
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(readTimeout)
                .build();
        httpGet.setConfig(requestConfig);

        try {
            // Execute the request
            HttpResponse response = httpClient.execute(httpGet);

            // Check the response status code
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                // Read and return the response content
                return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            } else {
                String errorMessage = "HTTP request failed with response code: " + statusCode;
                LOGGER.error(errorMessage);
                throw new IOException(errorMessage);
            }
        } finally {
            // Release resources
            httpGet.releaseConnection();
        }
    }

    private static String filterSpecialCharacters(String url) {
        // Remove or escape special characters as needed. Remove < and >
        url = url.replaceAll("[<>]", "");
        return url;
    }

    private static boolean isValidURL(String url) {
        // Use the regular expression pattern to validate the URL
        return URL_PATTERN.matcher(url).matches();
    }

    private static boolean containsPathTraversal(String url) {
        // Check for path traversal patterns in the URL
        return PATH_TRAVERSAL_PATTERN.matcher(url).find();
    }
}
