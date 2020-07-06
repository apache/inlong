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
      
#ifndef _TUBEMQ_CLIENT_CONST_RPC_H_
#define _TUBEMQ_CLIENT_CONST_RPC_H_

namespace tubemq {

using namespace std;


namespace rpc_config {

  // constant define
  static const int kRpcPrtBeginToken    = 0xFF7FF4FE;
  static const int kRpcMaxBufferSize    = 8192;
  static const int kRpcMaxFrameListCnt  = (int) ((1024 * 1024 * 8) / kRpcMaxBufferSize);
  // rpc protocol version
  static const int kRpcProtocolVersion  = 2;
  // msg type flag
  static const int kRpcFlagMsgRequest   = 0x0;
  static const int kRpcFlagMsgResponse  = 0x1;
  // service type
  static const int kMasterService      = 1;
  static const int kBrokerReadService  = 2;
  static const int kBrokerWriteService = 3;
  static const int kBrokerAdminService = 4;
  static const int kMasterAdminService = 5;
  // request method
  // master rpc method
  static const int kMasterMethoddProducerRegister = 1;
  static const int kMasterMethoddProducerHeatbeat = 2;
  static const int kMasterMethoddProducerClose    = 3;
  static const int kMasterMethoddConsumerRegister = 4;
  static const int kMasterMethoddConsumerHeatbeat = 5;
  static const int kMasterMethoddConsumerClose    = 6;
  // broker rpc method
  static const int kBrokerMethoddProducerRegister    = 11;
  static const int kBrokerMethoddProducerHeatbeat    = 12;
  static const int kBrokerMethoddProducerSendMsg     = 13;
  static const int kBrokerMethoddProducerClose       = 14;
  static const int kBrokerMethoddConsumerRegister    = 15;
  static const int kBrokerMethoddConsumerHeatbeat    = 16;
  static const int kBrokerMethoddConsumerGetMsg      = 17;
  static const int kBrokerMethoddConsumerCommit      = 18;  
  static const int kBrokerMethoddConsumerClose       = 19; 

  // register operate type
  static const int kRegOpTypeRegister       = 31; 
  static const int kRegOpTypeUnReg          = 32; 

  // rpc connect node timeout
  static const int kRpcConnectTimeoutMs    = 3000;
  
  // rpc timeout define  
  static const int kRpcTimoutDefSec = 15;
  static const int kRpcTimoutMaxSec = 300;
  static const int kRpcTimoutMinSec = 8;


}


}

#endif

