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

import socket


class SocketChannelFactory(object):
	def open_channel(self, host, port):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((host, port))
		return SocketChannel(sock)


class SocketChannel(object):
	def __init__(self, sock):
		self.sock = sock
		self.connected = True

	def write_bytes(self, bytes_stream):
		try:
			self.sock.sendall(bytes_stream)
		except socket.error:
			self.close()
			raise Exception("Socked send failed. Closing.")

	def read_bytes(self, n):
		data = self.sock.recv(n)
		return data

	def close(self):
		if self.connected:
			self.sock.close()
			self.connected = False
