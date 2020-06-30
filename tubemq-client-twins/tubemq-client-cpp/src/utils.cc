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

#include <vector>
#include <stdlib.h>
#include "utils.h"

namespace TubeMQ {


string Utils::trim(const string& source) {
  string target = source;
  if(!target.empty()) {
    size_t foudPos = target.find_first_not_of(tWhitespaceCharSet);
    if (foudPos != string::npos) {
      target = target.substr(foudPos);
    }
    foudPos = target.find_last_not_of(tWhitespaceCharSet);
    if(foudPos != string::npos) {
      target = target.substr(0, foudPos + 1);
    }
  }
  return target;
}

void Utils::split(const string& source, map<string, int>& result, 
      const string& delimiterStep1, const string& delimiterStep2) {
  int tmpValue;
  string subStr;
  string keyStr;
  string valStr;
  string::size_type pos1,pos2,pos3;
  if(!source.empty()) {
    pos1 = 0;
    pos2 = source.find(delimiterStep1);
    while(string::npos != pos2) {
      subStr = source.substr(pos1, pos2-pos1);
      subStr = Utils::trim(subStr);
      pos1 = pos2 + delimiterStep1.length();
      pos2 = source.find(delimiterStep1, pos1);
      if(subStr.empty()) {
        continue;
      }
      pos3 = subStr.find(delimiterStep2);
      if(string::npos == pos3) {
        continue;
      }
      keyStr = subStr.substr(0, pos3);
      valStr = subStr.substr(pos3+delimiterStep2.length());
      keyStr = Utils::trim(keyStr);
      valStr = Utils::trim(valStr);
      if(keyStr.empty()) {
        continue;
      }
      result[keyStr] = atoi(valStr.c_str());
    }
    if(pos1 != source.length()) {
      subStr = source.substr(pos1);
      subStr = Utils::trim(subStr);
      pos3 = subStr.find(delimiterStep2);
      if(string::npos != pos3) {
        keyStr = subStr.substr(0, pos3);
        valStr = subStr.substr(pos3+delimiterStep2.length());
        keyStr = Utils::trim(keyStr);
        valStr = Utils::trim(valStr);
        if(!keyStr.empty()){
          result[keyStr] = atoi(valStr.c_str());
        }
      }
    }
  }
}

}

