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

#include "moc_server.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#pragma pack(1)
typedef struct tag_tc_msg_head_st
{
    uint32_t total_len;
    char msg_type;
} tc_msg_head_st;

typedef struct tag_tc_msg_body_st
{
    uint32_t body_len;
    char body[0];
} tc_msg_body_st;

typedef struct tag_tc_msg_tail_st
{
    uint32_t attr_len;
    char attr[0];
} tc_msg_tail_st;

typedef struct tag_tc_msg_st
{
    tc_msg_head_st head;
    tc_msg_body_st body;
    tc_msg_tail_st tail;
} tc_msg_st;
#pragma pack()

#define TC_MSG_HEARTBEAT 1
#define TC_MSG_NOACKDATA 2
#define TC_MSG_ACKDATA 3
#define TC_MSG_ACK 4
#define TC_MSG_MULDATA 5

#define OK 0
#define ERR 1

#define INVLAID_SOCK -1
#define MAX_CONNECT 500
#define RECV_LEN 1024 * 1024 * 10

#define tc_log(fmt, args...)                                                                                                     \
    do                                                                                                                           \
    {                                                                                                                            \
        char* ch;                                                                                                                \
        char tm[100];                                                                                                            \
        time_t timep;                                                                                                            \
        time(&timep);                                                                                                            \
        ctime_r(&timep, &tm[0]);                                                                                                 \
        ch  = rindex(tm, '\n');                                                                                                  \
        *ch = 0;                                                                                                                 \
        printf("[%s]%s[%d]<%s>: " fmt, tm, __FILE__, __LINE__, __FUNCTION__, ##args);                                            \
    } while (0)

char recv_buf[RECV_LEN];

char send_buf[RECV_LEN];

int32_t listen_sock_fd = INVLAID_SOCK;

uint64_t get_mstime()
{
    uint64_t ms_time = 0;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    ms_time = (tv.tv_sec * 1000 + tv.tv_usec / 1000);

    return ms_time;
}

int32_t ack_data(int32_t sock_fd, char* attr, int32_t attr_len)
{
    int32_t ret;
    int32_t body_len         = 1;
    int32_t total_len        = 0;
    tc_msg_head_st* msg_head = NULL;
    tc_msg_body_st* msg_body = NULL;
    tc_msg_tail_st* msg_tail = NULL;

    memset(&send_buf[0], 0x0, RECV_LEN);

    msg_head           = (tc_msg_head_st*)send_buf;
    msg_head->msg_type = TC_MSG_ACKDATA;

    msg_body           = (tc_msg_body_st*)(send_buf + sizeof(tc_msg_head_st));
    msg_body->body_len = htonl(body_len);
    msg_body->body[0]  = '2';

    msg_tail = (tc_msg_tail_st*)(send_buf + sizeof(tc_msg_head_st) + sizeof(tc_msg_body_st) + body_len);
    snprintf(&msg_tail->attr[0], RECV_LEN, "%s&t=%llu", attr, get_mstime());
    msg_tail->attr_len = htonl(strlen(msg_tail->attr));

    total_len           = sizeof(tc_msg_st) + body_len + strlen(msg_tail->attr);
    msg_head->total_len = htonl(total_len - 4);

    ret = send(sock_fd, send_buf, total_len, 0);
    if (ret < 0)
    {
        tc_log("send data err, errno:%d.\n", errno);
        return ERR;
    }

    return OK;
}


int32_t proc_data(int32_t sock_fd, char* recv_buf, int32_t recv_len)
{
    // int32_t ret;
    int32_t proc_len = 0;
    int32_t tot_len  = 0;
    int32_t body_len = 0;
    int32_t tail_len = 0;

    tc_msg_head_st* head;
    tc_msg_body_st* body;
    // tc_msg_tail_st* tail;

#if 1
    for (;;)
    {
        if (proc_len >= recv_len) { break; }

        head = (tc_msg_head_st*)&recv_buf[proc_len];
        if ((head->msg_type > TC_MSG_MULDATA) || (head->msg_type < TC_MSG_HEARTBEAT))
        {
            tc_log("not support msg type(%d).\n", head->msg_type);
            return ERR;
        }

        tot_len  = ntohl(head->total_len);
        body     = (tc_msg_body_st*)(&recv_buf[proc_len] + sizeof(tc_msg_head_st));
        body_len = ntohl(body->body_len);
        if (body_len > tot_len)
        {
            tc_log("msg body len(%d) more than total len(%d).\n", body_len, tot_len);
            return ERR;
        }

        char* p_tail = &recv_buf[proc_len] + sizeof(tc_msg_head_st) + sizeof(tc_msg_body_st) + body_len;
        memcpy(&tail_len, p_tail, 4);
        tail_len = ntohl(tail_len);
        p_tail += 4;

        if (tail_len > tot_len)
        {
            tc_log("msg attr len(%d) more than total len(%d).\n", tail_len, tot_len);
            return ERR;
        }

        if ((tot_len + 4) != (sizeof(tc_msg_st) + tail_len + body_len))
        {
            tc_log("msg len err, total(%d) != 9 + body(%d) + tail(%d).\n", tot_len + 4, body_len, tail_len);
            tc_log("print tail:%s", recv_buf[proc_len + 5 + 4 + body_len]);
            tc_log("print attr: %s", p_tail);
            return ERR;
        }

        if (TC_MSG_ACKDATA || TC_MSG_MULDATA == head->msg_type)
        {
            tc_log("data need ack. attr:%s\n", p_tail);
            ack_data(sock_fd, p_tail, tail_len);
        }

        proc_len += (tot_len + 4);
    }
#endif

    return OK;
}

void recv_data(int32_t sock_fd, int32_t epoll_fds)
{
    int32_t ret;
    int32_t recv_len = 0;
    struct epoll_event event;

    memset(&recv_buf[0], 0x0, RECV_LEN);

    recv_len = recv(sock_fd, &recv_buf[0], RECV_LEN, 0);
    /* 对方关闭sock，需要剔除 */
    if (0 == recv_len)
    {
        tc_log("sock(%d) recv 0.\n", sock_fd);
        goto err_ret;
    }
    else if (recv_len < 0)
    {
        tc_log("sock recv less 0, err(%d).\n", errno);
        return;
    }

    ret = proc_data(sock_fd, &recv_buf[0], recv_len);
    if (ERR == ret)
    {
        tc_log("proc data err, sock(%d) will close.\n", sock_fd);
        goto err_ret;
    }

    return;

err_ret:
    event.events  = EPOLLIN | EPOLLET;
    event.data.fd = sock_fd;
    epoll_ctl(epoll_fds, EPOLL_CTL_DEL, sock_fd, &event);

    close(sock_fd);
    return;
}

void accept_connect(int32_t ser_fd, int32_t epoll_fds)
{
    int32_t new_fd;
    struct sockaddr_in sin;
    struct epoll_event event;
    socklen_t len = sizeof(struct sockaddr_in);

    bzero(&sin, len);

    new_fd = accept(ser_fd, (struct sockaddr*)&sin, &len);
    if (new_fd < 0)
    {
        tc_log("bad accept.\n");
        return;
    }

    tc_log("accetp new(%d) connect ip(%s) port(%d).\n", new_fd, inet_ntoa(sin.sin_addr), htons(sin.sin_port));

    event.events  = EPOLLIN | EPOLLET;
    event.data.fd = new_fd;
    epoll_ctl(epoll_fds, EPOLL_CTL_ADD, new_fd, &event);

    return;
}

/*
 * 接收任务
 */
int32_t recv_task()
{
    int32_t i       = 0;
    int32_t ret     = ERR;
    int32_t timeout = 3000;
    /* epoll描述符 */
    int32_t epoll_fds = 0;

    struct epoll_event event;
    struct epoll_event event_list[MAX_CONNECT];

    /* 接收前监听sock必须有效 */
    for (;;)
    {
        if (listen_sock_fd != INVLAID_SOCK) { break; }

        usleep(10000);
    }

    /* 开始监听 */
    ret = listen(listen_sock_fd, 5);
    if (ret < 0)
    {
        tc_log("listen sock(%d) err(%d).\n", listen_sock_fd, errno);
        goto err_ret;
    }

    /* 创建epoll fd */
    epoll_fds     = epoll_create(MAX_CONNECT);
    event.events  = EPOLLIN | EPOLLET;
    event.data.fd = listen_sock_fd;

    ret = epoll_ctl(epoll_fds, EPOLL_CTL_ADD, listen_sock_fd, &event);
    if (ret < 0)
    {
        tc_log("epoll add listen sock(%d) err(%d).\n", listen_sock_fd, errno);
        goto err_ret;
    }

    tc_log("epoll add listen sock(%d) ok.\n", listen_sock_fd);

    for (;;)
    {
        /* 监控所有的sock */
        ret = epoll_wait(epoll_fds, event_list, MAX_CONNECT, timeout);
        if (ret < 0)
        {
            tc_log("epoll wait err(%d).\n", errno);
            goto err_ret;
        }
        else if (0 == ret)
        {
            continue;
        }

        /* 处理监听收到的事件 */
        for (i = 0; i < ret; i++)
        {
            if ((event_list[i].events & EPOLLERR) || (event_list[i].events & EPOLLHUP) || !(event_list[i].events & EPOLLIN))
            {
                tc_log("epoll event(%x) err.\n", event_list[i].events);
                close(event_list[i].data.fd);
                goto err_ret;
            }

            if (event_list[i].data.fd == listen_sock_fd) { accept_connect(listen_sock_fd, epoll_fds); }
            else
            {
                recv_data(event_list[i].data.fd, epoll_fds);
            }
        }
    }

    return OK;

err_ret:
    if (INVLAID_SOCK != listen_sock_fd) { close(listen_sock_fd); }

    if (INVLAID_SOCK != epoll_fds) { close(epoll_fds); }

    return ERR;
}

/*
 * 创建监听的socket
 */
int32_t init_listen_sock(uint16_t port)
{
    int32_t ret = ERR;
    // int32_t  bufsize = RECV_LEN;
    int32_t sock_fd = INVLAID_SOCK;
    struct sockaddr_in ser_addr;

    /* 监听已经启动 */
    if (INVLAID_SOCK != listen_sock_fd)
    {
        tc_log("already start listen sock.\n");
        return ERR;
    }

    /* 创建socket */
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0)
    {
        tc_log("sock create err:%d.\n", errno);
        return ERR;
    }

    memset(&ser_addr, 0, sizeof(struct sockaddr_in));

    ser_addr.sin_family = AF_INET;
    ser_addr.sin_port   = htons(port);
    // inet_aton("192.168.146.100", &(ser_addr.sin_addr));
    inet_aton("127.0.0.1", &(ser_addr.sin_addr));
    // inet_aton("11.45.17.175", &(ser_addr.sin_addr));

    /* 绑定IP地址 */
    ret = bind(sock_fd, (struct sockaddr*)&ser_addr, sizeof(ser_addr));
    if (ret < 0)
    {
        tc_log("sock bind err(%d).\n", errno);
        return ERR;
    }

    // setsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, (char*)&bufsize, sizeof(int32_t));
    // setsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, (char*)&bufsize, sizeof(int32_t));

    tc_log("start listen on port(%d).\n", port);

    listen_sock_fd = sock_fd;

    return OK;
}
