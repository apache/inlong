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
import { ColumnsType } from 'antd/es/table';
import StaffSelect from '@/components/StaffSelect';
import rulesPattern from '@/utils/pattern';

export const getCreateFormContent = (defaultValues = {}) => {
  const array = [
    {
      type: 'input',
      label: 'DB连接名',
      name: 'serverName',
      rules: [{ required: true }],
      props: {
        maxLength: 128,
      },
    },
    {
      type: 'select',
      label: 'DB类型',
      name: 'dbType',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: 'MySQL',
            value: 'MySQL',
          },
          {
            label: 'PG',
            value: 'PG',
          },
        ],
      },
    },
    {
      type: 'input',
      label: 'DB IP',
      name: 'dbIp',
      rules: [
        { required: true },
        {
          pattern: rulesPattern.ip,
          message: '请正确输入Ip地址',
        },
      ],
    },
    {
      type: 'inputnumber',
      label: 'DB端口',
      name: 'dbPort',
      rules: [
        { required: true },
        {
          pattern: rulesPattern.port,
          message: '请正确输入端口号(1-65535)',
        },
      ],
    },
    {
      type: 'input',
      label: '备份DB IP',
      name: 'backupDbIp',
      rules: [
        {
          pattern: rulesPattern.ip,
          message: '请正确输入Ip地址',
        },
      ],
    },
    {
      type: 'inputnumber',
      label: '备份DB端口',
      name: 'backupDbPort',
      rules: [
        {
          pattern: rulesPattern.port,
          message: '请正确输入端口号(1-65535)',
        },
      ],
    },
    {
      type: 'input',
      label: '目标库',
      name: 'dbName',
    },
    {
      type: 'input',
      label: '用户名',
      name: 'username',
      rules: [{ required: true }],
    },
    {
      type: 'input',
      label: '密码',
      name: 'password',
      rules: [{ required: true }],
    },
    {
      type: <StaffSelect mode="multiple" currentUserClosable={false} />,
      label: 'DB负责人',
      name: 'inCharges',
      rules: [{ required: true, message: '请选择DB责任人' }],
    },
    {
      type: 'input',
      label: 'DB描述',
      name: 'dbDescription',
      props: {
        maxLength: 256,
      },
    },
  ];

  return array;
};

export const tableColumns: ColumnsType = [
  {
    title: 'DB连接名',
    dataIndex: 'serverName',
  },
  {
    title: 'DB类型',
    dataIndex: 'dbType',
  },
  {
    title: 'DB IP',
    dataIndex: 'dbIp',
  },
  {
    title: 'DB端口',
    dataIndex: 'dbPort',
  },
  {
    title: '目标库',
    dataIndex: 'dbName',
  },
  {
    title: 'DB描述',
    dataIndex: 'dbDescription',
  },
];
