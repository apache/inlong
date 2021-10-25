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

package org.apache.inlog.sort.examples;

import org.apache.inlog.sort.core.JobExecutor;
import org.apache.inlog.sort.core.JobInfo;

public class BasicDemoExample {

  private static final String DEMO_JOB = ""
      + "CREATE TABLE events (\n"
      + "  f_type INT,\n"
      + "  f_uid INT,\n"
      + "  ts AS localtimestamp,\n"
      + "  WATERMARK FOR ts AS ts\n"
      + ") WITH (\n"
      + "  'connector' = 'datagen',\n"
      + "  'rows-per-second'='5',\n"
      + "  'fields.f_type.min'='1',\n"
      + "  'fields.f_type.max'='5',\n"
      + "  'fields.f_uid.min'='1',\n"
      + "  'fields.f_uid.max'='1000'\n"
      + ");"
      + "\n"
      + "CREATE TABLE print_table (\n"
      + "  type INT,\n"
      + "  uid INT,\n"
      + "  lstmt TIMESTAMP\n"
      + ") WITH (\n"
      + "  'connector' = 'print',\n"
      + "  'sink.parallelism' = '2'\n"
      + ");"
      + "\n"
      + "CREATE TABLE print_table2 (\n"
      + "  type INT,\n"
      + "  uid INT,\n"
      + "  lstmt TIMESTAMP\n"
      + ") WITH (\n"
      + "  'connector' = 'print',\n"
      + "  'sink.parallelism' = '2'\n"
      + ");"
      + "\n"
      + "INSERT INTO print_table SELECT * FROM events where f_type = 1;"
      + "\n"
      + "INSERT INTO print_table2 SELECT * FROM events where f_type = 3;";

  public static void main(String[] args) throws Exception {

    JobInfo jobInfo = new JobInfo("demo_job", DEMO_JOB);
    JobExecutor.runJob(jobInfo);

  }

}
