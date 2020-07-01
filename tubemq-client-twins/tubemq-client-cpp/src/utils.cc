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
#include <sstream> 
#include <regex.h>
#include <stdlib.h>
#include "utils.h"
#include "const_config.h"


namespace tubemq {

static const string kWhitespaceCharSet = " \n\r\t\f\v";

string Utils::Trim(const string& source) {
  string target = source;
  if (!target.empty()) {
    size_t foud_pos = target.find_first_not_of(kWhitespaceCharSet);
    if (foud_pos != string::npos) {
      target = target.substr(foud_pos);
    }
    foud_pos = target.find_last_not_of(kWhitespaceCharSet);
    if (foud_pos != string::npos) {
      target = target.substr(0, foud_pos + 1);
    }
  }
  return target;
}

void Utils::Split(const string& source, vector<string>& result, const string& delimiter) {
  string item_str;
  string::size_type pos1,pos2;
  result.clear();
  if (!source.empty()) {
    pos1 = 0;
    pos2 = source.find(delimiter);
    while(string::npos != pos2) {
      item_str = Utils::Trim(source.substr(pos1, pos2-pos1));
      pos1 = pos2 + delimiter.size();
      pos2 = source.find(delimiter, pos1);
      if (!item_str.empty()) {
        result.push_back(item_str);
      }
    }
    if (pos1 != source.length())
    {
      item_str = Utils::Trim(source.substr(pos1));
      if (!item_str.empty()) {
        result.push_back(item_str);
      }
    }
  }
}


void Utils::Split(const string& source, map<string, int>& result, 
                const string& delimiter_step1, const string& delimiter_step2) {
  string item_str;
  string key_str;
  string val_str;
  string::size_type pos1,pos2,pos3;
  if (!source.empty()) {
    pos1 = 0;
    pos2 = source.find(delimiter_step1);
    while(string::npos != pos2) {
      item_str = source.substr(pos1, pos2-pos1);
      item_str = Utils::Trim(item_str);
      pos1 = pos2 + delimiter_step1.length();
      pos2 = source.find(delimiter_step1, pos1);
      if (item_str.empty()) {
        continue;
      }
      pos3 = item_str.find(delimiter_step2);
      if (string::npos == pos3) {
        continue;
      }
      key_str = item_str.substr(0, pos3);
      val_str = item_str.substr(pos3+delimiter_step2.length());
      key_str = Utils::Trim(key_str);
      val_str = Utils::Trim(val_str);
      if (key_str.empty()) {
        continue;
      }
      result[key_str] = atoi(val_str.c_str());
    }
    if (pos1 != source.length()) {
      item_str = source.substr(pos1);
      item_str = Utils::Trim(item_str);
      pos3 = item_str.find(delimiter_step2);
      if (string::npos != pos3) {
        key_str = item_str.substr(0, pos3);
        val_str = item_str.substr(pos3+delimiter_step2.length());
        key_str = Utils::Trim(key_str);
        val_str = Utils::Trim(val_str);
        if (!key_str.empty()){
          result[key_str] = atoi(val_str.c_str());
        }
      }
    }
  }
}

void Utils::Join(const vector<string>& vec, const string& delimiter, string& target) {
  vector<string>::const_iterator it;
  target.clear();
  for (it = vec.begin(); it != vec.end(); ++it) {
    target += *it;
    if (it != vec.end() - 1) {
      target += delimiter;
    }
  }
}

bool Utils::ValidString(string& err_info, const string& source, 
                bool allow_empty, bool pat_match, bool check_max_length, 
                unsigned int maxlen) {
  if (source.empty()) {
    if (allow_empty) {
      err_info = "Ok";
      return true;
    }
    err_info = "value is empty";
    return false;
  }
  if (check_max_length) {
    if (source.length() > maxlen) {
      stringstream ss;
      ss << source;
      ss << " over max length, the max allowed length is ";
      ss << maxlen;
      err_info = ss.str();
      return false;
    }
  }

  if (pat_match) {
    int cflags =REG_EXTENDED;     
    regex_t reg;    
    regmatch_t pmatch[1];
    const char* patRule = "^[a-zA-Z]\\w+$";  
    regcomp(&reg, patRule,cflags);
    int status = regexec(&reg, source.c_str(), 1, pmatch, 0);
    regfree(&reg);
    if (status == REG_NOMATCH) {
      stringstream ss;
      ss << source;
      ss << " must begin with a letter,can only contain characters,numbers,and underscores";
      err_info = ss.str();
      return false;
    }
  }
  err_info = "Ok";
  return true;        
}

bool Utils::ValidGroupName(string& err_info, 
                const string& group_name, string& tgt_group_name) {
  tgt_group_name = Utils::Trim(group_name);
  if (tgt_group_name.empty()) {
    err_info = "Illegal parameter: group_name is blank!";
    return false;
  }
  if (tgt_group_name.length() > config::kGroupNameMaxLength) {
    stringstream ss;
    ss << "Illegal parameter: ";
    ss << group_name;
    ss << " over max length, the max allowed length is ";
    ss << config::kGroupNameMaxLength;
    err_info = ss.str();
    return false;
  }
  int cflags =REG_EXTENDED;     
  regex_t reg;    
  regmatch_t pmatch[1];
  const char* patRule = "^[a-zA-Z][\\w-]+$"; 
  regcomp(&reg, patRule,cflags);
  int status = regexec(&reg, tgt_group_name.c_str(), 1, pmatch, 0);
  regfree(&reg);
  if (status == REG_NOMATCH) {
    stringstream ss;
    ss << "Illegal parameter: ";
    ss << group_name;
    ss << " must begin with a letter,can only contain ";
    ss << "characters,numbers,hyphen,and underscores";
    err_info = ss.str();
    return false;
  }
  err_info = "Ok";
  return true;        
}

bool Utils::ValidFilterItem(string& err_info, 
                const string& src_filteritem, string& tgt_filteritem) {
  tgt_filteritem = Utils::Trim(src_filteritem);
  if (tgt_filteritem.empty()) {
    err_info = "value is blank!";
    return false;
  }

  if (tgt_filteritem.length() > config::kFilterItemMaxLength) {
    stringstream ss;
    ss << "value over max length ";
    ss << config::kFilterItemMaxLength;
    err_info = ss.str();
    return false;
  }
  int cflags =REG_EXTENDED;    
  regex_t reg;    
  regmatch_t pmatch[1];
  const char* patRule = "^[_A-Za-z0-9]+$";  
  regcomp(&reg, patRule,cflags);
  int status = regexec(&reg, tgt_filteritem.c_str(), 1, pmatch, 0);
  regfree(&reg);
  if (status == REG_NOMATCH) {
      err_info = "value only contain characters,numbers,and underscores";
      return false;
  }
  err_info = "Ok";
  return true;      
}




}

