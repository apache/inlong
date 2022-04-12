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

package org.apache.inlong.manager.client.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;

import java.io.File;

@Parameters(commandDescription = "Create resource by json file")
public class CommandCreate extends CommandBase {

    public CommandCreate() {
        super("create");
        jcommander.addCommand("group", new CommandCreate.CreateGroup());
    }

    @Parameter()
    private java.util.List<String> params;

    @Parameters(commandDescription = "Create group by json file")
    private class CreateGroup extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-f", "--file"},
                converter = FileConverter.class,
                required = true,
                description = "json file")
        private File file;

        @Override
        void run() {
            try {
                String jsonFile = readFile(file);
                if (!jsonFile.isEmpty()) {
                    CreateGroupConf groupConf = jsonToObject(jsonFile);

                    InlongClient inlongClient = connect();
                    InlongGroup group = inlongClient.forGroup(groupConf.getGroupConf());
                    InlongStreamBuilder streamBuilder = group.createStream(groupConf.getStreamConf());
                    streamBuilder.fields(groupConf.getStreamFieldList());
                    streamBuilder.source(groupConf.getStreamSource());
                    streamBuilder.sink(groupConf.getStreamSink());
                    streamBuilder.initOrUpdate();
                    group.init();
                    System.out.println("Create group success!");
                }
            } catch (Exception e) {
                System.out.println("Create group failed!");
                System.out.println(e.getMessage());
            }
        }
    }
}
