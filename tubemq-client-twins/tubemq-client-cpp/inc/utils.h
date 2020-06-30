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

namespace TubeMQ {

  using namespace std;

  static const string tWhitespaceCharSet = " \n\r\t\f\v";

  namespace delimiter {
    static const string tDelimiterEqual = "=";
    static const string tDelimiterAnd   = "&";
    static const string tDelimiterComma = ",";
    static const string tDelimiterColon = ":";
    static const string tDelimiterAt    = "@";
    static const string tDelimiterPound = "#";
  }

  class Utils {
   public:
    // trim string info
    static string trim(const string& source);
    // split string to vector
    static void split(const string& source, map<string, int>& result, 
                     const string& delimiterStep1, const string& delimiterStep2);

  };
 
}

#endif

