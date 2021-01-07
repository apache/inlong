/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.core.conf;

import java.io.Closeable;
import org.apache.tubemq.agent.conf.AgentConfiguration;
import org.apache.tubemq.agent.conf.JobProfile;
import org.apache.tubemq.agent.constants.AgentConstants;
import org.apache.tubemq.agent.core.job.JobManager;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * start http server and get job/agent config via http
 */
public class ConfigJetty implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigJetty.class);

    private final AgentConfiguration conf;
    private final Server server;
    private final JobManager jobManager;

    public ConfigJetty(JobManager jobManager) {
        this.conf = AgentConfiguration.getAgentConf();
        this.jobManager = jobManager;
        server = new Server();
        try {
            initJetty();
        } catch (Exception ex) {
            LOGGER.error("exception caught", ex);
        }
    }

    private void initJetty() throws Exception {
        ServerConnector connector = new ServerConnector(this.server);
        connector.setPort(conf.getInt(
            AgentConstants.AGENT_HTTP_PORT, AgentConstants.DEFAULT_AGENT_HTTP_PORT));
        server.setConnectors(new Connector[] { connector });

        ServletHandler servletHandler = new ServletHandler();
        ServletHolder holder = new ServletHolder(new ConfigServlet(this));
        servletHandler.addServletWithMapping(holder, "/config/*");
        server.setHandler(servletHandler);
        server.start();
    }

    public void storeJobConf(JobProfile jobProfile) {
        // store job conf to bdb
        jobManager.submitJobProfile(jobProfile);
    }

    public void storeAgentConf(String confJsonStr) {
        // store agent conf to local file
        AgentConfiguration conf = AgentConfiguration.getAgentConf();
        conf.loadJsonStrResource(confJsonStr);
        conf.flushToLocalPropertiesFile();
    }

    @Override
    public void close() {
        try {
            if (this.server != null) {
                this.server.stop();
            }
        } catch (Exception ex) {
            LOGGER.error("exception caught", ex);
        }
    }
}
