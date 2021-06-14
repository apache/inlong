/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.corebase.config.constants;

public interface TLSCfgConst extends CommonCfgConst {

    /**
     * ----------------------------------------------------
     * Tls constants.
     * ----------------------------------------------------
     */
    String TLS_ENABLE = "tlsEnable";

    String TLS_PORT = "tlsPort";

    String TLS_KEY_STORE_PATH = "tlsKeyStorePath";

    String TLS_KEY_STORE_PASSWORD = "tlsKeyStorePassword";

    String TLS_TWO_WAY_AUTH_ENABLE = "tlsTwoWayAuthEnable";

    String TLS_TRUST_STORE_PATH = "tlsTrustStorePath";

    String TLS_TRUST_STORE_PASSWORD = "tlsTrustStorePassword";

    /**
     * --------------------------------------------------------------
     * TLS default values.
     * --------------------------------------------------------------
     */

    /**
     * Default value of {@link #TLS_ENABLE}.
     */
    boolean DEFAULT_TLS_ENABLE = false;

    /**
     * Default value of {@link #TLS_TWO_WAY_AUTH_ENABLE}.
     */
    boolean DEFAULT_TLS_TWO_WAY_AUTH_ENABLE = false;

    /**
     * Default value of {@link #TLS_PORT}.
     */
    int DEFAULT_TLS_PORT = 8124;

    /**
     * Default value of {@link #TLS_KEY_STORE_PATH}.
     */
    String DEFAULT_TLS_KEY_STORE_PATH = "";

    /**
     * Default value of {@link #TLS_KEY_STORE_PASSWORD}.
     */
    String DEFAULT_TLS_KEY_STORE_PASSWORD = "";

    /**
     * Default value of {@link #TLS_TRUST_STORE_PATH}.
     */
    String DEFAULT_TLS_TRUST_STORE_PATH = "";

    /**
     * Default value of {@link #TLS_TRUST_STORE_PASSWORD}.
     */
    String DEFAULT_TLS_TRUST_STORE_PASSWORD = "";

}
