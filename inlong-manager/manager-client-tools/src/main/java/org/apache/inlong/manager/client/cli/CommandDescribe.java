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
import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;

import java.util.List;

@Parameters(commandDescription = "Display details of one or more resources.")
public class CommandDescribe extends CommandBase {

    public CommandDescribe() {
        super("describe");
        jcommander.addCommand("stream", new DescribeStream());
        jcommander.addCommand("group", new DescribeGroup());
        jcommander.addCommand("sink", new DescribeSink());
        jcommander.addCommand("source", new DescribeSource());
    }

    @Parameter()
    private java.util.List<String> params;

    @Parameters(commandDescription = "Get stream details")
    private class DescribeStream extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-g", "--group"}, required = true, description = "Get stream details by group id.")
        private String groupId;

        @Override
        void run() {
            InnerInlongManagerClient managerClient = connect();
            List<FullStreamResponse> fullStreamResponseList = managerClient.listStreamInfo(groupId);
            fullStreamResponseList.forEach(response -> printJson(response.getStreamInfo()));
        }
    }

    @Parameters(commandDescription = "Get group details")
    private class DescribeGroup extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-s", "--status"}, description = "Get group details by status.")
        private int status;

        @Parameter(names = {"-g", "--group"}, description = "Get group details by group id.")
        private String group;

        @Parameter(names = {"-n", "--num"}, description = "Max number of groups to display.")
        private int pageSize = 10;

        @Override
        void run() {
            InnerInlongManagerClient managerClient = connect();
            PageInfo<InlongGroupListResponse> groupPageInfo = managerClient.listGroups(group, status, 1, pageSize);
            groupPageInfo.getList().forEach(this::printJson);
        }
    }

    @Parameters(commandDescription = "Get sink details")
    private class DescribeSink extends CommandUtil {

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
            sinkListResponses.forEach(this::printJson);
        }
    }

    @Parameters(commandDescription = "Get source details")
    private class DescribeSource extends CommandUtil {

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
            sourceListResponses.forEach(this::printJson);
        }
    }
}
