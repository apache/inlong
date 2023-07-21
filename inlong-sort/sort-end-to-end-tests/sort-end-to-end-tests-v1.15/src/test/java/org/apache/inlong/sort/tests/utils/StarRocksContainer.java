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

package org.apache.inlong.sort.tests.utils;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashSet;
import java.util.Set;

/**
 * Docker container for StarRocks.
 */
@SuppressWarnings("rawtypes")
public class StarRocksContainer extends JdbcDatabaseContainer {

    public static final String IMAGE = "starrocks/allin1-ubi";
    public static final Integer STAR_ROCKS_QUERY_PORT = 9030;
    public static final Integer STAR_ROCKS_FD_HTTP_PORT = 8030;
    public static final Integer STAR_ROCKS_ED_HTTP_PORT = 8040;

    private static final String MY_CNF_CONFIG_OVERRIDE_PARAM_NAME = "MY_CNF";
    private static final String SETUP_SQL_PARAM_NAME = "SETUP_SQL";
    private static final String STAR_ROCKS_ROOT_USER = "root";

    private String databaseName = "test";
    private String username = "inlong";
    private String password = "inlong";

    public StarRocksContainer() {
        this(StarRocksVersion.V3_1);
    }

    public StarRocksContainer(StarRocksVersion version) {
        super(DockerImageName.parse(IMAGE + ":" + version.getVersion()).asCompatibleSubstituteFor("starrocks"));
        addExposedPort(STAR_ROCKS_QUERY_PORT);
        addExposedPort(STAR_ROCKS_FD_HTTP_PORT);
        addExposedPort(STAR_ROCKS_ED_HTTP_PORT);
    }

    @Override
    protected Set<Integer> getLivenessCheckPorts() {
        return new HashSet<>(getMappedPort(STAR_ROCKS_QUERY_PORT));
    }

    @Override
    protected void configure() {
        optionallyMapResourceParameterAsVolume(
                MY_CNF_CONFIG_OVERRIDE_PARAM_NAME, "/data/deploy/", "/docker/starrocks/start_fe_be.sh");
        setStartupAttempts(1);
    }

    @Override
    public String getDriverClassName() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return "com.mysql.cj.jdbc.Driver";
        } catch (ClassNotFoundException e) {
            return "com.mysql.jdbc.Driver";
        }
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getDatabasePort()
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(databaseName);
    }

    public int getDatabasePort() {
        return getMappedPort(STAR_ROCKS_QUERY_PORT);
    }

    @Override
    protected String constructUrlForConnection(String queryString) {
        String url = super.constructUrlForConnection(queryString);

        if (!url.contains("useSSL=")) {
            String separator = url.contains("?") ? "&" : "?";
            url = url + separator + "useSSL=false";
        }

        if (!url.contains("allowPublicKeyRetrieval=")) {
            url = url + "&allowPublicKeyRetrieval=true";
        }

        return url;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    @SuppressWarnings("unchecked")
    public StarRocksContainer withConfigurationOverride(String s) {
        parameters.put(MY_CNF_CONFIG_OVERRIDE_PARAM_NAME, s);
        return this;
    }

    @SuppressWarnings("unchecked")
    public StarRocksContainer withSetupSQL(String sqlPath) {
        parameters.put(SETUP_SQL_PARAM_NAME, sqlPath);
        return this;
    }

    @Override
    public StarRocksContainer withDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Override
    public StarRocksContainer withUsername(final String username) {
        this.username = username;
        return this;
    }

    @Override
    public StarRocksContainer withPassword(final String password) {
        this.password = password;
        return this;
    }

    /** MySql version enum. */
    public enum StarRocksVersion {

        V2_5("2.5.8"),
        V3_0("3.0.4"),
        V3_1("3.1.0-rc01");

        private String version;

        StarRocksVersion(String version) {
            this.version = version;
        }

        public String getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "StarRocksVersion{" + "version='" + version + '\'' + '}';
        }
    }
}
