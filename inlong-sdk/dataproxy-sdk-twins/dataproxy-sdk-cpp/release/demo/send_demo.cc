/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "../../include/inlong_api.h"
#include <chrono>
#include <iostream>
#include <string>

using namespace std;
using namespace inlong;

// user set call_back func
int call_back_func(const char *inlong_group_id, const char *inlong_stream_id,
                   const char *msg, int msg_len, long report_time,
                   const char *ip) {
  cout << "******this is call back, print info******" << endl;
  cout << "inlong_group_id: " << inlong_group_id
       << ", inlong_stream_id: " << inlong_stream_id << endl;
  cout << "msg_len: " << msg_len << ", msg content: " << msg << endl;
  cout << "report_time: " << report_time << ", client ip: " << ip << endl;
  cout << "******call back end******" << endl;

  return 0;
}

int main(int argc, char const *argv[]) {
  if (argc < 2) {
    cout << "USAGE: ./send_demo ../config/config_example.json" << endl;
    return 0;
  }
  InLongApi inlong_api;
  // step1. init
  if (inlong_api.InitApi(argv[1])) {
    cout << "init error" << endl;
    return 0;
  }
  cout << "---->start sdk successfully" << endl;

  int count = 1000;
  string inlong_group_id = "test_cpp_sdk";
  string inlong_stream_id = "stream1";
  if (4 == argc) {
    inlong_group_id = argv[2];
    inlong_stream_id = argv[3];
  }
  cout << "inlong_group_id: " << inlong_group_id
       << ",inlong_stream_id:" << inlong_stream_id << endl;
  string msg =
      "this is a test ttttttttttttttt; eiwhgreuhg jfdiowaehgorerlea; test end";

  // step2. send
  cout << "---->start tc_api_send" << endl;
  for (size_t i = 0; i < count; i++) {
    if (inlong_api.Send(inlong_group_id.c_str(), inlong_stream_id.c_str(),
                        msg.c_str(), msg.length(), call_back_func)) {
      cout << "tc_api_send error;"
           << " ";
    }
  }

  // HTTP report example
  std::string http_url = "http://127.0.0.1:8080/inlong/dataproxy/send";
  std::string http_body = R"({\"groupId\":\"test_cpp_sdk\",\"streamId\":\"stream1\",\"msg\":\"this is a http test\"})";
  int http_ret = inlong_api.SendHttp(http_url.c_str(), http_body.c_str(), 5);
  if (http_ret == 0) {
    cout << "HTTP report succeeded" << endl;
  } else {
    cout << "HTTP report failed, code=" << http_ret << endl;
  }

  // step3. close
  if (inlong_api.CloseApi(10000)) {
    cout << "close sdk error" << endl;
  } else {
    cout << "---->close sdk successfully" << endl;
  }

  return 0;
}
