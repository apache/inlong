/*
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

import React from 'react';
import { Button } from 'antd';
import { Link } from 'react-router-dom';
import { DashTotal, DashToBeAssigned, DashPending, DashRejected } from '@/components/Icons';
import {
  statusList,
  genStatusTag,
  lastConsumerStatusList,
  genLastConsumerStatusTag,
} from './status';
import { timestampFormat } from '@/utils';

export const dashCardList = [
  {
    desc: '消费总数',
    dataIndex: 'totalCount',
    icon: <DashTotal />,
  },
  {
    desc: '待分配',
    dataIndex: 'waitingAssignCount',
    icon: <DashToBeAssigned />,
  },
  {
    desc: '待审批',
    dataIndex: 'waitingApproveCount',
    icon: <DashPending />,
  },
  {
    desc: '已驳回',
    dataIndex: 'rejectedCount',
    icon: <DashRejected />,
  },
];

export const getFilterFormContent = defaultValues =>
  [
    {
      type: 'inputsearch',
      name: 'keyword',
      props: {
        placeholder: '请输入关键词',
      },
    },
    {
      type: 'select',
      label: '申请状态',
      name: 'status',
      initialValue: defaultValues.status,
      props: {
        dropdownMatchSelectWidth: false,
        options: statusList,
      },
    },
    {
      type: 'select',
      label: '运行状态',
      name: 'lastConsumerStatus',
      initialValue: defaultValues.lastConsumerStatusList,
      props: {
        dropdownMatchSelectWidth: false,
        options: lastConsumerStatusList,
      },
    },
  ].map(item => {
    if (item.type === 'radio' || item.type === 'select') {
      return {
        type: 'select',
        label: item.label,
        name: item.name,
        initialValue: defaultValues[item.name as string],
        props: {
          allowClear: true,
          options: item.props.options,
          dropdownMatchSelectWidth: false,
        },
      };
    }
    return item;
  });

export const getColumns = ({ onDelete }) => {
  const genCreateUrl = record => `/consume/create?id=${record.id}`;
  const genDetailUrl = record =>
    record.status === 10 ? genCreateUrl(record) : `/consume/detail/${record.id}`;

  return [
    {
      title: '消费Topic',
      dataIndex: 'topic',
      width: 180,
      render: (text, record) => <Link to={genDetailUrl(record)}>{text}</Link>,
    },
    {
      title: '消费组',
      dataIndex: 'consumerGroupId',
      width: 180,
    },
    {
      title: '消息中间件',
      dataIndex: 'middlewareType',
      width: 120,
    },
    {
      title: '消费业务ID',
      dataIndex: 'businessIdentifier',
    },
    {
      title: '最近消费时间',
      dataIndex: 'lastConsumerTime',
      render: text => text && timestampFormat(text),
    },
    {
      title: '申请状态',
      dataIndex: 'status',
      render: text => genStatusTag(text),
      width: 120,
    },
    {
      title: '运行状态',
      dataIndex: 'lastConsumerStatus',
      render: text => text && genLastConsumerStatusTag(text),
      width: 120,
    },
    {
      title: '操作',
      dataIndex: 'action',
      render: (text, record) => (
        <>
          <Button type="link">
            <Link to={genDetailUrl(record)}>详情</Link>
          </Button>
          {[20, 22].includes(record?.status) && (
            <Button type="link">
              <Link to={genCreateUrl(record)}>修改</Link>
            </Button>
          )}
          <Button type="link" onClick={() => onDelete(record)}>
            删除
          </Button>
        </>
      ),
    },
  ];
};
