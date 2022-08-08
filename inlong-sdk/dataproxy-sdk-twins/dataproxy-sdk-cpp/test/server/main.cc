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

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "moc_server.h"

int main(int argc, char const* argv[])
{
    int32_t port;

    if (argc != 2)
    {
        printf("usage: ./main <port>\n");
        exit(0);
    }

    port = atoi(argv[1]);
    if (port <= 1000 || port > 65535)
    {
        printf("error: port(%d) is invalid, it should be in (1000, 65535]\n", port);
        printf("usage: ./moc_server <port>\n");
        exit(0);
    }

    printf("--->start moc server at port(%d).\n", port);

    signal(SIGPIPE, SIG_IGN);

    init_listen_sock((int16_t)port);

    printf("--->start moc server task.\n");

    recv_task();

    return 0;
    return 0;
}
