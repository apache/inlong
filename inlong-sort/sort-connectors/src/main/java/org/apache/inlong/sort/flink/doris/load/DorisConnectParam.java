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

package org.apache.inlong.sort.flink.doris.load;

import org.apache.commons.codec.binary.Base64;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class DorisConnectParam implements Serializable {
    private String username;
    private String password;
    private String streamLoadUrl;
    private String database;
    private String tableName;
    private String basicAuthStr;
    private Properties streamLoadProp;

    public DorisConnectParam(String streamLoadUrl,String username,
                             String password, String database,
                             String tableName, Properties streamLoadProp) {
        this.username = username;
        this.password = password;
        this.database = database;
        this.tableName = tableName;
        this.streamLoadProp = streamLoadProp;
        this.streamLoadUrl = streamLoadUrl;
        // build auth basic
        this.basicAuthStr = basicAuthHeader(username,password);
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getStreamLoadUrl() {
        return streamLoadUrl;
    }

    public void setStreamLoadUrl(String streamLoadUrl) {
        this.streamLoadUrl = streamLoadUrl;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getBasicAuthStr() {
        return basicAuthStr;
    }

    public void setBasicAuthStr(String basicAuthStr) {
        this.basicAuthStr = basicAuthStr;
    }

    public Properties getStreamLoadProp() {
        return streamLoadProp;
    }

    public void setStreamLoadProp(Properties streamLoadProp) {
        this.streamLoadProp = streamLoadProp;
    }
}
