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
import org.apache.inlong.manager.client.cli.util.PrintUtils;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;

import java.util.List;

/**
 * Get commond describe info of resources.
 */
@Parameters(commandDescription = "Display details of one or more resources")
public class CommandDescribe extends CommandBase {

    @Parameter()
    private java.util.List<String> params;

    public CommandDescribe() {
        super("describe");
        jcommander.addCommand("stream", new DescribeStream());
        jcommander.addCommand("group", new DescribeGroup());
        jcommander.addCommand("sink", new DescribeSink());
        jcommander.addCommand("source", new DescribeSource());
    }

    @Parameters(commandDescription = "Get stream details")
    private class DescribeStream extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-g", "--group"}, required = true, description = "inlong group id")
        private String groupId;

        @Override
        void run() {
            try {
                InnerInlongManagerClient managerClient = new InnerInlongManagerClient(connect().getConfiguration());
                List<FullStreamResponse> fullStreamResponseList = managerClient.listStreamInfo(groupId);
                fullStreamResponseList.forEach(response -> PrintUtils.printJson(response.getStreamInfo()));
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Get group details")
    private class DescribeGroup extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-s", "--status"})
        private int status;

        @Parameter(names = {"-g", "--group"}, description = "inlong group id")
        private String group;

        @Parameter(names = {"-n", "--num"}, description = "the number displayed")
        private int pageSize = 10;

        @Override
        void run() {
            try {
                InnerInlongManagerClient managerClient = new InnerInlongManagerClient(connect().getConfiguration());
                PageInfo<InlongGroupListResponse> groupPageInfo = managerClient.listGroups(group, status, 1, pageSize);
                groupPageInfo.getList().forEach(PrintUtils::printJson);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Get sink details")
    private class DescribeSink extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-s", "--stream"}, required = true, description = "inlong stream id")
        private String stream;

        @Parameter(names = {"-g", "--group"}, required = true, description = "inlong group id")
        private String group;

        @Override
        void run() {
            InnerInlongManagerClient managerClient = new InnerInlongManagerClient(connect().getConfiguration());
            try {
                List<SinkListResponse> sinkListResponses = managerClient.listSinks(group, stream);
                sinkListResponses.forEach(PrintUtils::printJson);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Get source details")
    private class DescribeSource extends CommandUtil {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-s", "--stream"}, required = true, description = "inlong stream id")
        private String stream;

        @Parameter(names = {"-g", "--group"}, required = true, description = "inlong group id")
        private String group;

        @Parameter(names = {"-t", "--type"}, description = "sink type")
        private String type;

        @Override
        void run() {
            try {
                InnerInlongManagerClient managerClient = new InnerInlongManagerClient(connect().getConfiguration());
                List<SourceListResponse> sourceListResponses = managerClient.listSources(group, stream, type);
                sourceListResponses.forEach(PrintUtils::printJson);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
