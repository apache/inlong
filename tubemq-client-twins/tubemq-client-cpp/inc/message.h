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
      
#ifndef _TUBEMQ_CLIENT_MESSAGE_H_
#define _TUBEMQ_CLIENT_MESSAGE_H_


#include <list>
#include <map>
#include <string>
#include <stdio.h>


namespace tubemq {

using namespace std;

class Message {
 public:
  Message();
  Message(const Message& target);
  Message(const string& topic, const char* data, int datalen);
  virtual ~Message();
  Message& operator=(const Message& target);
  const long GetMessageId() const;
  void SetMessageId(long message_id);
  const string& GetTopic() const;
  void SetTopic(const string& topic);
  const char* GetData() const;
  int GetDataLength() const;
  void setData(const char* data, int datalen);
  const int GetFlag() const;
  void SetFlag(int flag);
  const map<string, string>& GetProperties() const;
  int GetProperties(string& attribute);
  bool HasProperty(const string& key);
  bool GetProperty(const string& key, string& value);
  bool GetFilterItem(string& value);
  bool AddProperty(string& err_info, const string& key, const string& value);  

 private:
  void clearData();
  void copyData(const char* data, int datalen);  
  void copyProperties(const map<string, string>& properties);

  
 private:
  string topic_;
  char* data_;
  int   datalen_;
  long  message_id_;
  int   flag_;  
  map<string, string> properties_;
};

}




#endif

