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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

public class StarRocksManager {

    // ----------------------------------------------------------------------------------------
    // StarRocks Variables
    // ----------------------------------------------------------------------------------------
    public static final String INTER_CONTAINER_STAR_ROCKS_ALIAS = "starrocks";
    private static final String NEW_STARROCKS_REPOSITORY = "inlong-starrocks";
    private static final String NEW_STARROCKS_TAG = "latest";
    private static final String STAR_ROCKS_IMAGE_NAME = "starrocks/allin1-ubi:3.0.4";
    private static final String DEFAULT_PRIMARY_KEY = "id";
    public static final Logger STAR_ROCKS_LOG = LoggerFactory.getLogger(StarRocksContainer.class);

    static {
        GenericContainer oldStarRocks = new GenericContainer(STAR_ROCKS_IMAGE_NAME);
        Startables.deepStart(Stream.of(oldStarRocks)).join();
        oldStarRocks.copyFileToContainer(MountableFile.forClasspathResource("/docker/starrocks/start_fe_be.sh"),
                "/data/deploy/");
        try {
            oldStarRocks.execInContainer("chmod", "+x", "/data/deploy/start_fe_be.sh");
        } catch (Exception e) {
            e.printStackTrace();
        }
        oldStarRocks.getDockerClient()
                .commitCmd(oldStarRocks.getContainerId())
                .withRepository(NEW_STARROCKS_REPOSITORY)
                .withTag(NEW_STARROCKS_TAG).exec();
        oldStarRocks.stop();
    }

    public static String getNewStarRocksImageName() {
        return NEW_STARROCKS_REPOSITORY + ":" + NEW_STARROCKS_TAG;
    }

    public static void initializeStarRocksTable(StarRocksContainer STAR_ROCKS) {
        initializeStarRocksTable(STAR_ROCKS, DEFAULT_PRIMARY_KEY);
    }
    public static void initializeStarRocksTable(StarRocksContainer STAR_ROCKS, String primaryKey) {
        try (Connection conn =
                DriverManager.getConnection(STAR_ROCKS.getJdbcUrl(), STAR_ROCKS.getUsername(),
                        STAR_ROCKS.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute("CREATE TABLE IF NOT EXISTS test_output1 (\n"
                    + "       " + primaryKey + " INT NOT NULL,\n"
                    + "       name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                    + "       description VARCHAR(512)\n"
                    + ")\n"
                    + "PRIMARY KEY(" + primaryKey + ")\n"
                    + "DISTRIBUTED by HASH(" + primaryKey + ") PROPERTIES (\"replication_num\" = \"1\");");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
