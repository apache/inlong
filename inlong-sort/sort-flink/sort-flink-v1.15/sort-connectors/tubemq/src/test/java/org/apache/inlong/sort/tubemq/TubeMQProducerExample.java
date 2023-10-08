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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TubeMQProducerExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE tube_load_node (\n" +
                        "     id INT,\n" +
                        "     name STRING,\n" +
                        "     age INT,\n" +
                        "     salary FLOAT \n" +
                        "     ) WITH (\n" +
                        "     'connector' = 'tubemq',\n" +
                        "     'topic' = 'topic_sort_sink_test',\n" +
                        "     'master.rpc' = '127.0.0.1:8715',\n" +
                        "     'format' = 'json',\n" +
                        "     'group.id' = 'topic_sort_sink_test_group')");

        // define a insert
        TableResult tableResult1 = tEnv.executeSql("INSERT INTO tube_load_node VALUES (1, 'name_1', 31, 34.234)");
        TableResult tableResult2 = tEnv.executeSql("INSERT INTO tube_load_node VALUES (2, 'name_2', 32, 54.3)");
        TableResult tableResult3 = tEnv.executeSql("INSERT INTO tube_load_node VALUES (3, 'name_3', 33, 93.346)");
        TableResult tableResult4 = tEnv.executeSql("INSERT INTO tube_load_node VALUES (4, 'name_4', 34, 3.205)");
        TableResult tableResult5 = tEnv.executeSql("INSERT INTO tube_load_node VALUES (5, 'name_5', 35, 1.00)");
        // System.out.println(tableResult.getJobClient().get().getJobStatus());

        // env.execute();
    }
}
