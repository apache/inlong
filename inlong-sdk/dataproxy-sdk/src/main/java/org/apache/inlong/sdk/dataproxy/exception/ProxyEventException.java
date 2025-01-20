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
 * Proxy Event Exception
 *
 * This exception is used specifically when an unacceptable situation when constructing an event.
 * If this exception is thrown, the caller needs to solve the specified problem or discard the illegal message.
 */
public class ProxyEventException extends Exception {

    public ProxyEventException() {
    }

    public ProxyEventException(String message) {
        super(message);
    }

    public ProxyEventException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProxyEventException(Throwable cause) {
        super(cause);
    }
}
