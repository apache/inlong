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

namespace tubemq {

static const string kWhitespaceCharSet = " \n\r\t\f\v";

string Utils::Trim(const string& source) {
  string target = source;
  if(!target.empty()) {
    size_t foud_pos = target.find_first_not_of(kWhitespaceCharSet);
    if (foud_pos != string::npos) {
      target = target.substr(foud_pos);
    }
    foud_pos = target.find_last_not_of(kWhitespaceCharSet);
    if(foud_pos != string::npos) {
      target = target.substr(0, foud_pos + 1);
    }
  }
  return target;
}

void Utils::Split(const string& source, map<string, int>& result, 
      const string& delimiter_step1, const string& delimiter_step2) {
  string item_str;
  string key_str;
  string val_str;
  string::size_type pos1,pos2,pos3;
  if(!source.empty()) {
    pos1 = 0;
    pos2 = source.find(delimiter_step1);
    while(string::npos != pos2) {
      item_str = source.substr(pos1, pos2-pos1);
      item_str = Utils::Trim(item_str);
      pos1 = pos2 + delimiter_step1.length();
      pos2 = source.find(delimiter_step1, pos1);
      if(item_str.empty()) {
        continue;
      }
      pos3 = item_str.find(delimiter_step2);
      if(string::npos == pos3) {
        continue;
      }
      key_str = item_str.substr(0, pos3);
      val_str = item_str.substr(pos3+delimiter_step2.length());
      key_str = Utils::Trim(key_str);
      val_str = Utils::Trim(val_str);
      if(key_str.empty()) {
        continue;
      }
      result[key_str] = atoi(val_str.c_str());
    }
    if(pos1 != source.length()) {
      item_str = source.substr(pos1);
      item_str = Utils::Trim(item_str);
      pos3 = item_str.find(delimiter_step2);
      if(string::npos != pos3) {
        key_str = item_str.substr(0, pos3);
        val_str = item_str.substr(pos3+delimiter_step2.length());
        key_str = Utils::Trim(key_str);
        val_str = Utils::Trim(val_str);
        if(!key_str.empty()){
          result[key_str] = atoi(val_str.c_str());
        }
      }
    }
  }
}

}

