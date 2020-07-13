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

import unittest
from network.RPCProtocol import RPCProtocol
from network.SocketChannel import SocketChannelFactory
from network.RequestWrapper import RequestWrapper
import generated.MasterService_pb2 as master_pb2


class testRPCProtocol(unittest.TestCase):

    def test_RPCProtocol(self):
        socket_channel = SocketChannelFactory().open_channel("10.215.128.42", 8081)
        rpcProtocol = RPCProtocol()

        registerrequestp2m = master_pb2.RegisterRequestP2M()
        registerrequestp2m.clientId = "python-docker-1-1"
        registerrequestp2m.topicList.extend(["demo"])
        registerrequestp2m.brokerCheckSum = -1L
        registerrequestp2m.hostName = "127.0.0.1"
        # encode
        requestRapper = RequestWrapper(12345, 1, 1, registerrequestp2m)
        rpcProtocol.encode(socket_channel, requestRapper)
        # decode
        responce_data = rpcProtocol.decode(socket_channel).data_List[0]
        registerresponsem2p = master_pb2.RegisterResponseM2P()
        registerresponsem2p.ParseFromString(responce_data)
        self.assertTrue(registerresponsem2p.success, True)


if __name__ == '__main__':
    unittest.main()