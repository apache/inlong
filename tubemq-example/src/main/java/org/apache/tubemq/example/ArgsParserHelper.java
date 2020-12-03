/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.example;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class ArgsParserHelper {

    /**
     * Print help information and exit.
     *
     * @param opts - options
     */
    public static void help(String commandName, Options opts) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(commandName, opts);
        System.exit(0);
    }

    /**
     * Init common options when parsing args.
     * @return - options
     */
    public static Options initCommonOptions() {
        Options options = new Options();
        options.addOption(null, "help", false, "show help");
        options.addOption(null, "master-list", true, "master address like: host1:8000,host2:8000");
        options.addOption(null, "topic", true, "topic list, topic1,topic2 or "
                + "topic1:tid11;tid12,topic2:tid21;tid22(consumer only)");
        return options;
    }
}
