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
import { statusList, genStatusTag } from './status';
import { timestampFormat } from '@/utils';

export const getFilterFormContent = defaultValues => [
  {
    type: 'inputnumber',
    name: 'processInstId',
    props: {
      style: { width: 150 },
      min: 1,
      max: 100000000,
      placeholder: '请输入流程单ID',
    },
  },
  {
    type: 'inputsearch',
    label: '申请人',
    name: 'applicant',
  },
  {
    type: 'select',
    label: '状态',
    name: 'states',
    initialValue: defaultValues.states,
    props: {
      dropdownMatchSelectWidth: false,
      options: statusList,
      allowClear: true,
    },
  },
];

export const getColumns = activedName => [
  {
    title: '流程单ID',
    dataIndex: 'processInstId',
    render: (text, record) => (
      <Link to={`/approvals/detail/${text}?actived=${activedName}&taskId=${record.id}`}>
        {text}
      </Link>
    ),
  },
  {
    title: '申请人',
    dataIndex: 'applicant',
  },
  {
    title: '申请类型',
    dataIndex: 'processDisplayName',
  },
  {
    title: '业务ID',
    dataIndex: 'businessIdentifier',
    render: (text, record) => record.showInList?.businessIdentifier,
  },
  {
    title: '申请时间',
    dataIndex: 'startTime',
    render: text => timestampFormat(text),
  },
  {
    title: '状态',
    dataIndex: 'state',
    render: text => genStatusTag(text),
  },
  {
    title: '操作',
    dataIndex: 'action',
    render: (text, record) => (
      <Link
        to={`/approvals/detail/${record.processInstId}?actived=${activedName}&taskId=${record.id}`}
      >
        详情
      </Link>
    ),
  },
];
