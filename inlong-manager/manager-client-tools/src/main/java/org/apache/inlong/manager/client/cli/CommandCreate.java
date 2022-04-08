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
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;

import java.io.File;

@Parameters()
public class CommandCreate extends CommandBase {

    public CommandCreate() {
        super("create");
        jcommander.addCommand("stream", new CommandCreate.CreateStream());
        jcommander.addCommand("group", new CommandCreate.CreateGroup());
        jcommander.addCommand("sink", new CommandCreate.CreateSink());
        jcommander.addCommand("source", new CommandCreate.CreateSource());
    }

    @Parameter()
    private java.util.List<String> params;

    @Parameters(commandDescription = "Create stream by json file")
    private class CreateStream extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-f", "--file"}, converter = FileConverter.class, required = true)
        private File file;

        @Override
        void run() {
            try {
                String json = readFile(file);
                if (!json.isEmpty()) {
                    InnerInlongManagerClient managerClient = connect();
                    InlongStreamInfo inlongStreamInfo = jsonToObject(json, InlongStreamInfo.class);
                    String stream = managerClient.createStreamInfo(inlongStreamInfo);
                    System.out.println(stream);
                    System.out.println("Create stream success!");
                } else {
                    System.out.println("Create stream failed!");
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Create group by json file")
    private class CreateGroup extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-f", "--file"}, converter = FileConverter.class, required = true)
        private File file;

        @Override
        void run() {
            try {
                String json = readFile(file);
                if (!json.isEmpty()) {
                    InlongGroupRequest inlongGroupRequest = jsonToObject(json, InlongGroupRequest.class);
                    InnerInlongManagerClient managerClient = connect();
                    String group = managerClient.createGroup(inlongGroupRequest);
                    System.out.println(group);
                    System.out.println("Create group success!");
                } else {
                    System.out.println("Create group failed!");
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Create group by json file")
    private class CreateSink extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-f", "--file"}, converter = FileConverter.class, required = true)
        private File file;

        @Override
        void run() {
            try {
                String json = readFile(file);
                if (!json.isEmpty()) {
                    InnerInlongManagerClient managerClient = connect();
                    SinkRequest sinkRequest = jsonToObject(json, SinkRequest.class);
                    String sink = managerClient.createSink(sinkRequest);
                    System.out.println(sink);
                    System.out.println("Create sink success!");
                } else {
                    System.out.println("Create sink failed!");
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Create group by json file")
    private class CreateSource extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-f", "--file"}, converter = FileConverter.class, required = true)
        private File file;

        @Override
        void run() {
            try {
                String json = readFile(file);
                if (!json.isEmpty()) {
                    InnerInlongManagerClient managerClient = connect();
                    SourceRequest sourceRequest = jsonToObject(json, SourceRequest.class);
                    String source = managerClient.createSource(sourceRequest);
                    System.out.println(source);
                    System.out.println("Create group success!");
                } else {
                    System.out.println("Create group failed!");
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
