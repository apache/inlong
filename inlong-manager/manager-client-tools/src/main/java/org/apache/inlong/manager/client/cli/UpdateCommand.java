
package org.apache.inlong.manager.client.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.cli.pojo.CreateGroupConf;
import org.apache.inlong.manager.client.cli.util.ClientUtils;

import java.io.File;

@Parameters(commandDescription = "Update resource by json file")
public class UpdateCommand extends AbstractCommand {
    @Parameter()
    private java.util.List<String> params;

    public UpdateCommand() {
        super("update");
        jcommander.addCommand("update", new UpdateCommand.UpdateGroup());
    }

    @Parameters(commandDescription = "Update group by json file")
    private static class UpdateGroup extends AbstractCommandRunner {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-f", "--file"},
                converter = FileConverter.class,
                required = true,
                description = "json file")
        private File file;

        @Override
        void run() {
            throw new NotImplementedException();
        }
    }
}
