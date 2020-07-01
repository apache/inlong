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
      
#ifndef _TUBEMQ_CLIENT_UTILS_H_
#define _TUBEMQ_CLIENT_UTILS_H_

#include <map>
#include <string>

namespace tubemq {

using namespace std;


namespace delimiter {
  static const string kDelimiterEqual = "=";
  static const string kDelimiterAnd   = "&";
  static const string kDelimiterComma = ",";
  static const string kDelimiterColon = ":";
  static const string kDelimiterAt    = "@";
  static const string kDelimiterPound = "#";
}

class Utils {
 public:
  // trim string info
  static string Trim(const string& source);
  // split string to vector
  static void Split(const string& source, map<string, int>& result, 
                   const string& delimiter_step1, const string& delimiter_step2);

};
 
}

#endif

