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
import org.apache.inlong.manager.client.api.enums.SimpleGroupStatus;
import org.apache.inlong.manager.client.api.inner.client.InlongGroupClient;
import org.apache.inlong.manager.client.api.inner.client.InlongStreamClient;
import org.apache.inlong.manager.client.api.inner.client.StreamSinkClient;
import org.apache.inlong.manager.client.api.inner.client.StreamSourceClient;
import org.apache.inlong.manager.client.cli.pojo.GroupInfo;
import org.apache.inlong.manager.client.cli.pojo.SinkInfo;
import org.apache.inlong.manager.client.cli.pojo.SourceInfo;
import org.apache.inlong.manager.client.cli.pojo.StreamInfo;
import org.apache.inlong.manager.client.cli.util.ClientUtils;
import org.apache.inlong.manager.client.cli.util.PrintUtils;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.common.pojo.sink.StreamSink;
import org.apache.inlong.manager.common.pojo.source.StreamSource;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;

import java.io.IOException;
import java.util.List;

/**
 * Get main information of resources.
 */
@Parameters(commandDescription = "Displays summary information about one or more resources")
public class ListCommand extends AbstractCommand {

    @Parameter()
    private List<String> params;

    public ListCommand() {
        super("list");

        try {
            ClientUtils.initClientFactory();
        } catch (IOException e) {
            System.err.println("init clientFactory error");
            System.err.println(e.getMessage());
            return;
        }

        jcommander.addCommand("stream", new ListStream(ClientUtils.clientFactory.getStreamClient()));
        jcommander.addCommand("group", new ListGroup(ClientUtils.clientFactory.getGroupClient()));
        jcommander.addCommand("sink", new ListSink(ClientUtils.clientFactory.getSinkClient()));
        jcommander.addCommand("source", new ListSource(ClientUtils.clientFactory.getSourceClient()));
    }

    @Parameters(commandDescription = "Get stream summary information")
    private static class ListStream extends AbstractCommandRunner {

        private final InlongStreamClient streamClient;

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-g", "--group"}, required = true, description = "inlong group id")
        private String groupId;

        ListStream(InlongStreamClient streamClient) {
            this.streamClient = streamClient;
        }

        @Override
        void run() {
            try {
                List<InlongStreamInfo> streamInfos = streamClient.listStreamInfo(groupId);
                PrintUtils.print(streamInfos, StreamInfo.class);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Get group summary information")
    private static class ListGroup extends AbstractCommandRunner {

        private static final int DEFAULT_PAGE_SIZE = 10;

        private final InlongGroupClient groupClient;

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-s", "--status"}, description = "inlong group status")
        private String status;

        @Parameter(names = {"-g", "--group"}, description = "inlong group id")
        private String group;

        @Parameter(names = {"-n", "--num"}, description = "the number displayed")
        private int pageSize;

        ListGroup(InlongGroupClient groupClient) {
            this.groupClient = groupClient;
        }

        @Override
        void run() {
            try {
                InlongGroupPageRequest pageRequest = new InlongGroupPageRequest();
                pageRequest.setKeyword(group);
                // set default page size to DEFAULT_PAGE_SIZE
                pageSize = pageSize <= 0 ? DEFAULT_PAGE_SIZE : pageSize;
                pageRequest.setPageNum(1).setPageSize(pageSize);

                // set default status to STARTED
                status = status == null ? SimpleGroupStatus.STARTED.toString() : status;
                List<Integer> statusList = SimpleGroupStatus.parseStatusCodeByStr(status);
                pageRequest.setStatusList(statusList);

                PageInfo<InlongGroupListResponse> groupPageInfo = groupClient.listGroups(pageRequest);
                PrintUtils.print(groupPageInfo.getList(), GroupInfo.class);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Get sink summary information")
    private static class ListSink extends AbstractCommandRunner {

        private final StreamSinkClient sinkClient;

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-s", "--stream"}, required = true, description = "stream id")
        private String stream;

        @Parameter(names = {"-g", "--group"}, required = true, description = "group id")
        private String group;

        ListSink(StreamSinkClient sinkClient) {
            this.sinkClient = sinkClient;
        }

        @Override
        void run() {
            try {
                List<StreamSink> streamSinks = sinkClient.listSinks(group, stream);
                PrintUtils.print(streamSinks, SinkInfo.class);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Get source summary information")
    private static class ListSource extends AbstractCommandRunner {

        private final StreamSourceClient sourceClient;

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-s", "--stream"}, required = true, description = "inlong stream id")
        private String stream;

        @Parameter(names = {"-g", "--group"}, required = true, description = "inlong group id")
        private String group;

        @Parameter(names = {"-t", "--type"}, description = "source type")
        private String type;

        ListSource(StreamSourceClient sourceClient) {
            this.sourceClient = sourceClient;
        }

        @Override
        void run() {
            try {
                List<StreamSource> streamSources = sourceClient.listSources(group, stream, type);
                PrintUtils.print(streamSources, SourceInfo.class);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
