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

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.util.Arrays;
import java.util.stream.Collectors;

/** Standalone containerized HBase instance that builds the image on the fly. */
@SuppressWarnings("rawtypes")
public class HBaseContainer extends GenericContainer<HBaseContainer> {

    private static final String HBASE_BIN = "/opt/hbase/bin";
    private static final int MAX_RETRIES = 3;

    public HBaseContainer(String hbaseVersion) {
        super(getImageFromDockerfile(hbaseVersion));
    }

    private static ImageFromDockerfile getImageFromDockerfile(String hbaseVersion) {
        return new ImageFromDockerfile()
                .withDockerfileFromBuilder(
                        builder -> builder.from("adoptopenjdk/openjdk8")
                                .env("HBASE_VERSION", hbaseVersion)
                                .run(
                                        "export INITRD=no"
                                                + " && export HBASE_DIST=\"http://archive.apache.org/dist/hbase\""
                                                + " && apt-get update -y"
                                                + " && apt-get install -y --no-install-recommends curl"
                                                + " && cd /opt"
                                                + " && curl -SL $HBASE_DIST/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz"
                                                + " | tar -x -z && mv hbase-${HBASE_VERSION} hbase")
                                .expose(2181)
                                .cmd(
                                        "/bin/sh",
                                        "-c",
                                        String.format(
                                                "nohup %s/start-hbase.sh & sleep infinity",
                                                HBASE_BIN)));
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        ExecResult res = null;
        for (int i = 0; i < MAX_RETRIES; i++) {
            try {
                res = execCmd("scan 'hbase:meta'");
                if (res.getStdout().contains("hbase:namespace")) {
                    return;
                }
                Thread.sleep(5000L);
            } catch (Exception e) {
                throw new RuntimeException("Failed to verify if container is started.", e);
            }
        }
        throw new IllegalStateException("Failed to start HBase properly:\n" + res);
    }

    public Container.ExecResult createTable(String table, String... colFamilies) throws Exception {
        String createCmd =
                String.format("create '%s',", table)
                        + Arrays.stream(colFamilies)
                                .map(cf -> String.format("{NAME=>'%s'}", cf))
                                .collect(Collectors.joining(","));

        return execCmd(createCmd);
    }

    public Container.ExecResult putData(
            String table, String rowKey, String colFamily, String colQualifier, String val)
            throws Exception {
        String putCmd =
                String.format(
                        "put '%s','%s','%s:%s','%s'", table, rowKey, colFamily, colQualifier, val);

        return execCmd(putCmd);
    }

    public Container.ExecResult scanTable(String table) throws Exception {
        String scanCmd = String.format("scan '%s'", table);

        return execCmd(scanCmd);
    }

    private Container.ExecResult execCmd(String cmd) throws Exception {
        String hbaseShellCmd = String.format("echo \"%s\" | %s/hbase shell", cmd, HBASE_BIN);

        return execInContainer("sh", "-c", hbaseShellCmd);
    }
}