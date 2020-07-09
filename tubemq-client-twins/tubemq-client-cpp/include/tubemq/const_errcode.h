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

#ifndef TUBEMQ_CLIENT_CONST_ERR_CODE_H_
#define TUBEMQ_CLIENT_CONST_ERR_CODE_H_

namespace tubemq {

namespace err_code {
  static const int32_t kErrSuccess = 200;
  static const int32_t kErrNotReady = 201;
  static const int32_t kErrMoved = 301;

  static const int32_t kErrBadRequest = 400;
  static const int32_t kErrUnAuthorized = 401;
  static const int32_t kErrForbidden = 403;
  static const int32_t kErrNotFound = 404;
  static const int32_t kErrPartitionOccupied = 410;
  static const int32_t kErrHbNoNode = 411;
  static const int32_t kErrDuplicatePartition = 412;
  static const int32_t kErrCertificateFailure = 415;
  static const int32_t kErrServerOverflow = 419;
  static const int32_t kErrConsumeGroupForbidden = 450;
  static const int32_t kErrConsumeSpeedLimit = 452;
  static const int32_t kErrConsumeContentForbidden = 455;
  
  static const int32_t kErrServerError = 500;
  static const int32_t kErrServiceUnavilable = 503;
  static const int32_t kErrServerMsgsetNullError = 510;
  static const int32_t kErrWaitServerRspTimeout = 550;
}  // namespace tubemq

#endif  // TUBEMQ_CLIENT_CONST_ERR_CODE_H_