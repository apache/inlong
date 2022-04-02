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

package org.apache.inlong.manager.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;

import java.util.List;

@Parameters(commandDescription = "Displays main information for one or more resources.")
public class CmdList extends CmdBase {

    public CmdList() {
        super("list");
        jcommander.addCommand("stream", new CmdList.ListStream());
        jcommander.addCommand("group", new CmdList.ListGroup());
        jcommander.addCommand("sink", new CmdList.ListSink());
        jcommander.addCommand("source", new CmdList.ListSource());
    }

    @Parameter()
    private java.util.List<String> params;

    @Parameters(commandDescription = "Get stream main information")
    private class ListStream extends CliCommand {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-g", "--group"}, required = true, description = "Get stream main information by group id.")
        private String groupId;

        @Override
        void run() {
            InnerInlongManagerClient managerClient = connect();
            List<FullStreamResponse> fullStreamResponseList = managerClient.listStreamInfo(groupId);
            print(fullStreamResponseList,FullStreamResponse.class);
        }
    }

    @Parameters(commandDescription = "Get group details")
    private class ListGroup extends CliCommand {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-s", "--status"})
        private int status;

        @Parameter(names = {"-g", "--group"})
        private String group;

        @Parameter(names = {"-n", "--num"})
        private int pageSize = 10;

        @Override
        void run() {
            InnerInlongManagerClient managerClient = connect();
            PageInfo<InlongGroupListResponse> groupPageInfo = managerClient.listGroups(group, status, 1, pageSize);
            print(groupPageInfo.getList(),InlongGroupListResponse.class);
        }
    }

    @Parameters(commandDescription = "Get sink details")
    private class ListSink extends CliCommand {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-s", "--stream"}, required = true, description = "stream id")
        private String stream;

        @Parameter(names = {"-g", "--group"}, required = true, description = "group id")
        private String group;

        @Override
        void run() {
            InnerInlongManagerClient managerClient = connect();
            List<SinkListResponse> sinkListResponses = managerClient.listSinks(group, stream);
            print(sinkListResponses,SinkListResponse.class);
        }
    }

    @Parameters(commandDescription = "Get source details")
    private class ListSource extends CliCommand {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-s", "--stream"}, required = true, description = "stream id")
        private String stream;

        @Parameter(names = {"-g", "--group"}, required = true, description = "group id")
        private String group;

        @Parameter(names = {"-t", "--type"}, required = true, description = "sink type")
        private String type;

        @Override
        void run() {
            InnerInlongManagerClient managerClient = connect();
            List<SourceListResponse> sourceListResponses = managerClient.listSources(group, stream, type);
            print(sourceListResponses,SourceListResponse.class);
        }
    }
}
