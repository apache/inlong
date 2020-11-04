#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from __future__ import print_function

import tubemq_client
import tubemq_config
import tubemq_errcode
import tubemq_return
import tubemq_message
import time

topic_list = set(['demo'])
master_addr = '127.0.0.1:8000'
group_name = 'test_group'
conf_file = './client.conf'
err_info = ''
result = False

py_consumer = tubemq_client.TubeMQConsumer()
consumer_config = tubemq_config.ConsumerConfig()
consumer_config.setRpcReadTimeoutMs(20000)

# set master addr
result = consumer_config.setMasterAddrInfo(err_info, master_addr)
if not result:
    print("Set Master AddrInfo failure:", err_info)
    exit(1)

# set master addr
result = consumer_config.setGroupConsumeTarget(err_info, group_name, topic_list)
if not result:
    print("Set GroupConsume Target failure:", err_info)
    exit(1)

# StartTubeMQService
result = tubemq_client.startTubeMQService(err_info, conf_file)
if not result:
    print("StartTubeMQService failure:", err_info)
    exit(1)

result = py_consumer.start(err_info, consumer_config)
if not result:
    print("Initial consumer failure, error is:", err_info)
    exit(1)

gentRet = tubemq_return.ConsumerResult()
confirm_result = tubemq_return.ConsumerResult()
start_time = time.time()
while True:
    result = py_consumer.getMessage(gentRet)
    if result:
        msgs = gentRet.getMessageList()
        print("GetMessage success, msssage count =", len(msgs))
        result = py_consumer.confirm(gentRet.getConfirmContext(), True, confirm_result)
    else:
        # 2.2.1 if failure, check error code
        # print error message if errcode not in
        # [no partitions assigned, all partitions in use,
        #    or all partitons idle, reach max position]
        if not gentRet.getErrCode() == tubemq_errcode.Result.kErrNotFound \
                or not gentRet.getErrCode() == tubemq_errcode.Result.kErrNoPartAssigned \
                or not gentRet.getErrCode() == tubemq_errcode.Result.kErrAllPartInUse \
                or not gentRet.getErrCode() == tubemq_errcode.Result.kErrAllPartWaiting:
            print('GetMessage failure, err_code=%d, err_msg is:%s', gentRet.getErrCode(), gentRet.getErrMessage())

    # used for test, consume 10 minutes only
    stop_time = time.time()
    if stop_time - start_time > 10 * 60:
        break;

# stop and shutdown
result = py_consumer.shutDown()
result = tubemq_client.stopTubeMQService(err_info)
if not result:
    print("StopTubeMQService failure, reason is:" + err_info)
    exit(1)
