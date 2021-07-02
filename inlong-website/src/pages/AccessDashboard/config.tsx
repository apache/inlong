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
import { Link } from 'react-router-dom';
import { DashTotal, DashToBeAssigned, DashPending, DashRejected } from '@/components/Icons';
import { statusList, genStatusTag } from './status';
import { Button } from 'antd';

export const dashCardList = [
  {
    desc: '接入总数',
    dataIndex: 'totalCount',
    icon: <DashTotal />,
  },
  {
    desc: '待分配',
    dataIndex: 'waitAssignCount',
    icon: <DashToBeAssigned />,
  },
  {
    desc: '待审批',
    dataIndex: 'waitApproveCount',
    icon: <DashPending />,
  },
  {
    desc: '已驳回',
    dataIndex: 'rejectCount',
    icon: <DashRejected />,
  },
];

export const getFilterFormContent = defaultValues => [
  {
    type: 'inputsearch',
    name: 'keyWord',
    initialValue: defaultValues.keyWord,
    props: {
      allowClear: true,
      placeholder: '请输入关键词',
    },
  },
  {
    type: 'select',
    label: '状态',
    name: 'status',
    initialValue: defaultValues.status,
    props: {
      allowClear: true,
      options: statusList,
      dropdownMatchSelectWidth: false,
    },
  },
];

export const getColumns = ({ onDelete, openModal }) => {
  const genCreateUrl = record => `/access/create?bid=${record.businessIdentifier}`;
  const genDetailUrl = record =>
    [0, 100].includes(record.status)
      ? genCreateUrl(record)
      : `/access/detail/${record.businessIdentifier}`;

  return [
    {
      title: '业务ID',
      dataIndex: 'businessIdentifier',
      render: (text, record) => <Link to={genDetailUrl(record)}>{text}</Link>,
    },
    {
      title: '业务名称',
      dataIndex: 'cnName',
    },
    {
      title: '责任人',
      dataIndex: 'inCharges',
    },
    {
      title: '创建时间',
      dataIndex: 'createTime',
    },
    {
      title: '状态',
      dataIndex: 'status',
      render: text => genStatusTag(text),
    },
    {
      title: '操作',
      dataIndex: 'action',
      render: (text, record) => (
        <>
          <Button type="link">
            <Link to={genDetailUrl(record)}>详情</Link>
          </Button>
          {[102].includes(record?.status) && (
            <Button type="link">
              <Link to={genCreateUrl(record)}>修改</Link>
            </Button>
          )}
          <Button type="link" onClick={() => onDelete(record)}>
            删除
          </Button>
          {record?.status && (record?.status === 120 || record?.status === 130) && (
            <Button type="link" onClick={() => openModal(record)}>
              执行日志
            </Button>
          )}
        </>
      ),
    },
  ];
};
