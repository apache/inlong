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

import multiprocessing


class RpcConstants(object):
    RPC_CODEC = "rpc.codec"
    BOSS_COUNT = "rpc.netty.boss.count"
    WORKER_COUNT = "rpc.netty.worker.count"
    CALLBACK_WORKER_COUNT = "rpc.netty.callback.count"
    CLIENT_POOL_POOL_COUNT = "rpc.netty.client.pool.count"
    CLIENT_CACHE_QUEUE_SIZE = "rpc.netty.client.cache.queue.size"
    WORKER_THREAD_NAME = "rpc.netty.worker.thread.name"
    CLIENT_ROLE_TYPE = "rpc.netty.client.role.type"
    WORKER_MEM_SIZE = "rpc.netty.worker.mem.size"
    SERVER_POOL_POOL_COUNT = "rpc.netty.server.pool.count"
    SERVER_CACHE_QUEUE_SIZE = "rpc.netty.server.cache.queue.size"
    SERVER_ROLE_TYPE = "rpc.netty.server.role.type"
    CONNECT_TIMEOUT = "rpc.connect.timeout"
    # "rpc.request.timeout"
    REQUEST_TIMEOUT = 10000
    READ_TIMEOUT = "rpc.read.timeout"
    WRITE_TIMEOUT = "rpc.write.timeout"
    CONNECT_READ_IDLE_DURATION = "rpc.connect.read.idle.duration"
    NETTY_WRITE_HIGH_MARK = "rpc.netty.write.highmark"
    NETTY_WRITE_LOW_MARK = "rpc.netty.write.lowmark"
    NETTY_TCP_SENDBUF = "rpc.netty.send.buffer"
    NETTY_TCP_RECEIVEBUF = "rpc.netty.receive.buffer"
    TCP_NODELAY = "rpc.tcp.nodelay"
    TCP_REUSEADDRESS = "rpc.tcp.reuseaddress"
    TCP_KEEP_ALIVE = "rpc.tcp.keeplive"
    TLS_OVER_TCP = "tcp.tls"
    TLS_TWO_WAY_AUTHENTIC = "tls.twoway.authentic"
    TLS_KEYSTORE_PATH = "tls.keystore.path"
    TLS_KEYSTORE_PASSWORD = "tls.keystore.password"
    TLS_TRUSTSTORE_PATH = "tls.truststore.path"
    TLS_TRUSTSTORE_PASSWORD = "tls.truststore.password"
    RPC_LQ_STATS_DURATION = "rpc.link.quality.stats.duration"
    RPC_LQ_FORBIDDEN_DURATION = "rpc.link.quality.forbidden.duration"
    RPC_LQ_MAX_ALLOWED_FAIL_COUNT = "rpc.link.quality.max.allowed.fail.count"
    RPC_LQ_MAX_FAIL_FORBIDDEN_RATE = "rpc.link.quality.max.fail.forbidden.rate"
    RPC_SERVICE_UNAVAILABLE_FORBIDDEN_DURATION = "rpc.unavailable.service.forbidden.duration"
    RPC_PROTOCOL_VERSION = 2
    RPC_PROTOCOL_BEGIN_TOKEN = -8391426
    RPC_MAX_BUFFER_SIZE = 8192
    MAX_FRAME_MAX_LIST_SIZE = int(((1024 * 1024 * 8) / RPC_MAX_BUFFER_SIZE))
    RPC_FLAG_MSG_TYPE_REQUEST = 0x0
    RPC_FLAG_MSG_TYPE_RESPONSE = 0x1
    RPC_SERVICE_TYPE_MASTER_SERVICE = 1
    RPC_SERVICE_TYPE_BROKER_READ_SERVICE = 2
    RPC_SERVICE_TYPE_BROKER_WRITE_SERVICE = 3
    RPC_SERVICE_TYPE_BROKER_ADMIN_SERVICE = 4
    RPC_SERVICE_TYPE_MASTER_ADMIN_SERVICE = 5
    RPC_MSG_MASTER_METHOD_BEGIN = 1
    RPC_MSG_MASTER_PRODUCER_REGISTER = 1
    RPC_MSG_MASTER_PRODUCER_HEARTBEAT = 2
    RPC_MSG_MASTER_PRODUCER_CLOSECLIENT = 3
    RPC_MSG_MASTER_CONSUMER_REGISTER = 4
    RPC_MSG_MASTER_CONSUMER_HEARTBEAT = 5
    RPC_MSG_MASTER_CONSUMER_CLOSECLIENT = 6
    RPC_MSG_MASTER_BROKER_REGISTER = 7
    RPC_MSG_MASTER_BROKER_HEARTBEAT = 8
    RPC_MSG_MASTER_BROKER_CLOSECLIENT = 9
    RPC_MSG_MASTER_METHOD_END = 9
    RPC_MSG_BROKER_METHOD_BEGIN = 11
    RPC_MSG_BROKER_PRODUCER_REGISTER = 11
    RPC_MSG_BROKER_PRODUCER_HEARTBEAT = 12
    RPC_MSG_BROKER_PRODUCER_SENDMESSAGE = 13
    RPC_MSG_BROKER_PRODUCER_CLOSE = 14
    RPC_MSG_BROKER_CONSUMER_REGISTER = 15
    RPC_MSG_BROKER_CONSUMER_HEARTBEAT = 16
    RPC_MSG_BROKER_CONSUMER_GETMESSAGE = 17
    RPC_MSG_BROKER_CONSUMER_COMMIT = 18
    RPC_MSG_BROKER_CONSUMER_CLOSE = 19

    MSG_OPTYPE_REGISTER = 31
    MSG_OPTYPE_UNREGISTER = 32
    CFG_RPC_READ_TIMEOUT_DEFAULT_MS = 10000
    CFG_RPC_READ_TIMEOUT_MAX_MS = 600000
    CFG_RPC_READ_TIMEOUT_MIN_MS = 3000
    CFG_CONNECT_READ_IDLE_TIME = 300000
    CFG_DEFAULT_BOSS_COUNT = 1
    CFG_DEFAULT_RSP_CALLBACK_WORKER_COUNT = 3
    CFG_DEFAULT_TOTAL_MEM_SIZE = 10485760
    CFG_DEFAULT_CLIENT_WORKER_COUNT = multiprocessing.cpu_count() + 1
    CFG_DEFAULT_SERVER_WORKER_COUNT = multiprocessing.cpu_count() * 2
    CFG_DEFAULT_WORKER_THREAD_NAME = "tube_rpc_netty_worker-"
    CFG_LQ_STATS_DURATION_MS = 60000
    CFG_LQ_FORBIDDEN_DURATION_MS = 1800000
    CFG_LQ_MAX_ALLOWED_FAIL_COUNT = 5
    CFG_LQ_MAX_FAIL_FORBIDDEN_RATE = 0.3
    CFG_UNAVAILABLE_FORBIDDEN_DURATION_MS = 50000
    CFG_DEFAULT_NETTY_WRITEBUFFER_HIGH_MARK = 50 * 1024 * 1024
    CFG_DEFAULT_NETTY_WRITEBUFFER_LOW_MARK = 5 * 1024 * 1024
    # added for python sdk
    INT_BYTE_NUM = 4

