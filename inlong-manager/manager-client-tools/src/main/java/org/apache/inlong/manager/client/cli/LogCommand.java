package org.apache.inlong.manager.client.cli;
/*
note: this log iterates over the resource list, will return nothing if the resource list is too large to iterate.
usage is limited to dev testing and small scale production, for now.
 */


import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.cli.pojo.CreateGroupConf;
import org.apache.inlong.manager.client.cli.util.ClientUtils;

import java.io.File;

/**
 * Log resource
 */
@Parameters(commandDescription = "Log resource")
public class LogCommand extends AbstractCommand {

    @Parameter()
    private java.util.List<String> params;

    public LogCommand() {
        super("log");
        jcommander.addCommand("group", new CreateGroup());
    }

    @Parameters(commandDescription = "Log group")
    private static class CreateGroup extends AbstractCommandRunner {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-f", "--file"},
                converter = FileConverter.class,
                description = "json file")
        private File file;

        @Parameter(names = {"-s"})
        private String input;

        @Override
        void run() {
            System.out.println("start running");
            try {
                String fileContent = ClientUtils.readFile(file);
                if (StringUtils.isBlank(fileContent)) {
                    System.out.println("Create group failed: file was empty!");
                    return;
                }
                //first extract groupconfig from the file passed in
                CreateGroupConf groupConf = objectMapper.readValue(fileContent, CreateGroupConf.class);
                //get the correspodning inlonggroup, a.k.a the task to execute
                InlongClient inlongClient = ClientUtils.getClient();
                InlongGroup group = inlongClient.forGroup(groupConf.getGroupInfo());
                InlongStreamBuilder streamBuilder = group.createStream(groupConf.getStreamInfo());
                //put in parameters:source and sink,stream fields, then initialize
                streamBuilder.fields(groupConf.getStreamFieldList());
                streamBuilder.source(groupConf.getStreamSource());
                streamBuilder.sink(groupConf.getStreamSink());
                streamBuilder.initOrUpdate();
                //initialize the new stream group
                group.init();
                System.out.println("Create group success!");
            } catch (Exception e) {
                System.out.println("Create group failed!");
                System.out.println(e.getMessage());
            }
        }
    }
}