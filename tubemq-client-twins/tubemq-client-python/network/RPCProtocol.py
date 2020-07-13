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

import struct
import generated.RPC_pb2 as rpc_pb2

from constants.RpcConstants import RpcConstants
from network.ResponseWrapper import ResponseWrapper
from util.logger import logger

from google.protobuf.internal.decoder import _DecodeVarint
from google.protobuf.internal.encoder import _VarintBytes


class RPCProtocol:
    def __init__(self):
        self.pb_const = RpcConstants()

    def encode(self, socket_channel, request_rapper):
        if not socket_channel.connected:
            logger().error("socket_channel is not connected")
            raise Exception
        # RpcConnHeader
        connectionHeader = rpc_pb2.RpcConnHeader()
        connectionHeader.flag = self.pb_const.RPC_FLAG_MSG_TYPE_REQUEST
        # RequestHeader
        rpcHeader = rpc_pb2.RequestHeader()
        rpcHeader.serviceType = request_rapper.service_type
        rpcHeader.protocolVer = self.pb_const.RPC_PROTOCOL_VERSION
        # RequestBody
        rpcBodyRequest = rpc_pb2.RequestBody()
        rpcBodyRequest.method = request_rapper.method
        rpcBodyRequest.timeout = self.pb_const.REQUEST_TIMEOUT
        rpcBodyRequest.request = request_rapper.request_data.SerializeToString()
        # stream bytes to send
        stream_bytes = ""
        for data in [RpcConstants().RPC_PROTOCOL_BEGIN_TOKEN, request_rapper.serial_no, 3]:
            stream_bytes = stream_bytes + struct.pack('>i', data)
        stream_bytes = stream_bytes + struct.pack('>L', len(connectionHeader.SerializeToString()) \
            + len(_VarintBytes(connectionHeader.ByteSize()))) + _VarintBytes(connectionHeader.ByteSize()) \
            + connectionHeader.SerializeToString() + struct.pack('>L', len(rpcHeader.SerializeToString()) \
            + len(_VarintBytes(rpcHeader.ByteSize()))) + _VarintBytes(rpcHeader.ByteSize()) + rpcHeader.SerializeToString() \
            + struct.pack('>L', len(rpcBodyRequest.SerializeToString()) + len(_VarintBytes(rpcBodyRequest.ByteSize()))) \
            + _VarintBytes(rpcBodyRequest.ByteSize()) + rpcBodyRequest.SerializeToString()
        socket_channel.write_bytes(stream_bytes)

    def decode(self, socket_channel):
        if not socket_channel.connected:
            logger().error("socket_channel is not connected")
            raise Exception

        frame_token = struct.unpack('>i', socket_channel.read_bytes(self.pb_const.INT_BYTE_NUM))[0]
        logger().info(frame_token)
        if frame_token != self.pb_const.RPC_PROTOCOL_BEGIN_TOKEN:
            logger().error("frame_token error")
            raise ValueError
        serial_no = struct.unpack('>i', socket_channel.read_bytes(self.pb_const.INT_BYTE_NUM))[0]
        data_size = struct.unpack('>i', socket_channel.read_bytes(self.pb_const.INT_BYTE_NUM))[0]
        response_rapper = ResponseWrapper(serial_no)

        for i in range(data_size):
            data_len = struct.unpack('>i', socket_channel.read_bytes(self.pb_const.INT_BYTE_NUM))[0]
            # TODO: check data length
            # RpcConnHeader
            rpcconn_header_size = _DecodeVarint(socket_channel.read_bytes(1), 0)[0]
            rpcconn_header_data = socket_channel.read_bytes(rpcconn_header_size)
            rpcconn_header = rpc_pb2.RpcConnHeader()
            rpcconn_header.ParseFromString(rpcconn_header_data)
            logger().debug(rpcconn_header)
            # ResponseHeader
            response_header_size = _DecodeVarint(socket_channel.read_bytes(1), 0)[0]
            response_header_data = socket_channel.read_bytes(response_header_size)
            response_header = rpc_pb2.ResponseHeader()
            response_header.ParseFromString(response_header_data)
            logger().debug(response_header)
            rsp_responsebody_size = _DecodeVarint(socket_channel.read_bytes(1), 0)[0]
            rsp_responsebody_data = socket_channel.read_bytes(rsp_responsebody_size)
            # 0 means success
            if response_header.status == 0:
                # RspResponseBody
                rsp_responsebody = rpc_pb2.RspResponseBody()
                rsp_responsebody.ParseFromString(rsp_responsebody_data)
                rsp_response_data = rsp_responsebody.data
                response_rapper.data_List.append(rsp_response_data)
            else:
                # RspExceptionBody
                rsp_responsebody = rpc_pb2.RspExceptionBody()
                rsp_response_data = rsp_responsebody.ParseFromString(rsp_responsebody_data)
                logger().error(rsp_response_data.exceptionName)
                logger().error(rsp_response_data.stackTrace)
        return response_rapper


