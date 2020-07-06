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

#ifndef _TUBEMQ_CLIENT_FLOW_CONTROL_H_
#define _TUBEMQ_CLIENT_FLOW_CONTROL_H_

#include <map>
#include <list>
#include <string>
#include <vector>
#include <algorithm>
#include <rapidjson/document.h>
#include "atomic_def.h"



namespace tubemq {

using namespace std;

class FlowCtrlResult {
 public:
  FlowCtrlResult();
  FlowCtrlResult(long datasize_limit, int freqms_limit);
  FlowCtrlResult& operator=(const FlowCtrlResult& target);
  void SetDataDltAndFreqLimit(long datasize_limit, int freqms_limit);
  void SetDataSizeLimit(long datasize_limit);
  void SetFreqMsLimit(int freqms_limit);
  long GetDataSizeLimit();
  int GetFreqMsLimit();

 private:
  long datasize_limit_;  
  int  freqms_limit_;
};


class FlowCtrlItem {
 public:
  FlowCtrlItem();
  FlowCtrlItem(int type,int zero_cnt,int freqms_limit);
  FlowCtrlItem(int type, 
    int datasize_limit,int freqms_limit,int min_data_filter_freqms);
  FlowCtrlItem(int type, int start_time, 
    int end_time, long datadlt_m, long datasize_limit, int freqms_limit);
  FlowCtrlItem& operator=(const FlowCtrlItem& target);
  void Clear();
  void ResetFlowCtrlValue(int type, 
    int datasize_limit,int freqms_limit,int min_data_filter_freqms);
  int GetFreLimit(int msg_zero_cnt);
  bool GetDataLimit(long datadlt_m, int curr_time, FlowCtrlResult& flowctrl_result);
  const int GetType() const {
    return type_;
  }
  const int GetZeroCnt() const {
    return zero_cnt_;
  }
  const int GetStartTime() const {
    return start_time_;
  }
  const int GetEndTime() const {
    return end_time_;
  }
  const long GetDataSizeLimit() const { 
    return datasize_limit_;
  }
  const int GetFreqMsLimit() const {
    return freqms_limit_;
  }
  const long GetDltInM() const {
    return datadlt_m_;
  }

 private:
  int  type_;
  int  start_time_;
  int  end_time_;
  long datadlt_m_;
  long datasize_limit_;
  int  freqms_limit_;
  int  zero_cnt_;
};

class FlowCtrlRuleHandler {
 public:
  FlowCtrlRuleHandler();
  ~FlowCtrlRuleHandler();
  void UpdateDefFlowCtrlInfo(bool is_default, 
    int qrypriority_id, long flowctrl_id, const string& flowctrl_info);
  bool GetCurDataLimit(long last_datadlt,FlowCtrlResult& flowctrl_result);
  int GetCurFreqLimitTime(int msg_zero_cnt, int received_limit);
  int GetMinZeroCnt() { return this->min_zero_cnt_.Get();}
  int GetQryPriorityId() { 
    return this->qrypriority_id_.Get();
  }
  void SetQryPriorityId(int qrypriority_id) { 
    this->qrypriority_id_.Set(qrypriority_id);
  }
  long GetFlowCtrlId() { 
    return this->flowctrl_id_.Get();
  }
  const FlowCtrlItem& GetFilterCtrlItem() const {
    return this->filter_ctrl_item_;
  }
  const string& GetFlowCtrlInfo() const { 
    return this->flowctrl_info_;
  }

 private:
  void initialStatisData();
  void clearStatisData();
  static bool compareFeqQueue(const FlowCtrlItem& queue1, const FlowCtrlItem& queue2);
  static bool compareDataLimitQueue(const FlowCtrlItem& o1, const FlowCtrlItem& o2);
  bool parseStringMember(string &err_info, const rapidjson::Value& root, 
    const char* key, string& value, bool compare_value, string required_val);
  bool parseLongMember(string &err_info, const rapidjson::Value& root, 
    const char* key, long& value, bool compare_value, long required_val);
  bool parseIntMember(string &err_info, const rapidjson::Value& root, 
    const char* key, int& value, bool compare_value, int required_val);
  bool parseFlowCtrlInfo(const string& flowctrl_info, map<int,vector<FlowCtrlItem> >& flowctrl_info_map);
  bool parseDataLimit(string& err_info, const rapidjson::Value& root, vector<FlowCtrlItem>& flowCtrlItems);
  bool parseFreqLimit(string& err_info, const rapidjson::Value& root, vector<FlowCtrlItem>& flowctrl_items);
  bool parseLowFetchLimit(string& err_info, const rapidjson::Value& root, vector<FlowCtrlItem>& flowctrl_items);
  bool parseTimeMember(string& err_info, const rapidjson::Value& root, const char* key, int& value);

 private:
  AtomicLong    flowctrl_id_;
  AtomicInteger qrypriority_id_;
  string        flowctrl_info_;
  AtomicInteger min_zero_cnt_;
  AtomicLong    min_datadlt_limt_;
  AtomicInteger datalimit_start_time_;
  AtomicInteger datalimit_end_time_;
  FlowCtrlItem  filter_ctrl_item_;
  map<int, vector<FlowCtrlItem> > flowctrl_rules_;
  pthread_rwlock_t configrw_lock_;
  long last_update_time_;
};

  

}


#endif

