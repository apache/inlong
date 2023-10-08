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

package org.apache.inlong.sort.tubemq;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TubeMQConsumerExample {

    public static void main(String[] args) throws Exception {
        // final ParameterTool params = ParameterTool.fromArgs(args);
        // final String hostname = params.get("hostname", "localhost");
        // final String port = params.get("port", "9999");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register a table in the catalog
        tEnv.executeSql(
                "CREATE TABLE tube_extract_node (\n" +
                        "     id INT,\n" +
                        "     name STRING,\n" +
                        "     age INT,\n" +
                        "     salary FLOAT \n" +
                        "     ) WITH (\n" +
                        "     'connector' = 'tubemq',\n" +
                        "     'topic' = 'topic_sort_test',\n" +
                        "     'master.rpc' = '127.0.0.1:8715',\n" +
                        "     'format' = 'json',\n" +
                        "     'group.id' = 'topic_sort_test_group')");

        // define a dynamic query
        final Table result = tEnv.sqlQuery("SELECT * FROM tube_extract_node");

        // print the result to the console
        tEnv.toDataStream(result).print();
        // tEnv.toChangelogStream(result).print();

        env.execute();
    }
}
