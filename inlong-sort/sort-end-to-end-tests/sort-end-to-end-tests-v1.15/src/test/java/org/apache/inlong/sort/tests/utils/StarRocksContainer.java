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

import org.apache.commons.lang3.StringUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.joining;

/**
 * Docker container for StarRocks.
 */
@SuppressWarnings("rawtypes")
public class StarRocksContainer extends GenericContainer {

    public static final String IMAGE = "starrocks/allin1-ubi";
    public static final Integer STAR_ROCKS_QUERY_PORT = 9030;
    public static final Integer STAR_ROCKS_FD_HTTP_PORT = 8030;
    public static final Integer STAR_ROCKS_ED_HTTP_PORT = 8040;

    private Map<String, String> urlParameters = new HashMap<>();

    private String databaseName = "test";
    private String username = "inlong";
    private String password = "inlong";

    public StarRocksContainer() {
        this(StarRocksVersion.V3_0);
    }

    public StarRocksContainer(StarRocksVersion version) {
        super(DockerImageName.parse(IMAGE + ":" + version.getVersion()).asCompatibleSubstituteFor("starrocks"));
        addExposedPort(STAR_ROCKS_QUERY_PORT);
        addExposedPort(STAR_ROCKS_FD_HTTP_PORT);
        addExposedPort(STAR_ROCKS_ED_HTTP_PORT);
    }

    public StarRocksContainer(String imageName) {
        super(DockerImageName.parse(imageName).asCompatibleSubstituteFor("starrocks"));
        addExposedPort(STAR_ROCKS_QUERY_PORT);
        addExposedPort(STAR_ROCKS_FD_HTTP_PORT);
        addExposedPort(STAR_ROCKS_ED_HTTP_PORT);
    }

    public String getDriverClassName() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return "com.mysql.cj.jdbc.Driver";
        } catch (ClassNotFoundException e) {
            return "com.mysql.jdbc.Driver";
        }
    }

    private String constructUrlParameters(String startCharacter, String delimiter, String endCharacter) {
        String urlParameters = "";
        if (!this.urlParameters.isEmpty()) {
            String additionalParameters = this.urlParameters.entrySet().stream()
                    .map(Object::toString)
                    .collect(joining(delimiter));
            urlParameters = startCharacter + additionalParameters + endCharacter;
        }
        return urlParameters;
    }
    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&", StringUtils.EMPTY);
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getDatabasePort()
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    public String getJdbcUrl() {
        return getJdbcUrl(databaseName);
    }

    public int getDatabasePort() {
        return getMappedPort(STAR_ROCKS_QUERY_PORT);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    protected String getTestQueryString() {
        return "SELECT 1";
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
