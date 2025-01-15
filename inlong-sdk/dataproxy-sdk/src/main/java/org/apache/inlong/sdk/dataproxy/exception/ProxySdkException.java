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

package org.apache.inlong.sdk.dataproxy.exception;

/**
 * Proxy Sdk Exception
 *
 * Used for unacceptable situations when reporting messages, such as empty input parameters,
 * illegal parameters, abnormal execution status, and exceptions encountered during execution that
 * were not considered during design, etc.
 *
 * If this exception is thrown during the debugging phase, the caller needs to check and
 * adjust the corresponding implementation according to the exception content; if the exception
 * is encountered during operation; the caller can try a limited number of times,
 * and discard this report if it fails after trying again.
 */
public class ProxySdkException extends Exception {

    public ProxySdkException() {
    }

    public ProxySdkException(String message) {
        super(message);
    }

    public ProxySdkException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProxySdkException(Throwable cause) {
        super(cause);
    }
}
