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
import { genStatusTag } from './status';
import { timestampFormat } from '@/utils';

export const getFilterFormContent = () => [
  {
    type: 'inputsearch',
    name: 'keyword',
    props: {
      placeholder: '请输入关键词',
    },
  },
];

export const getColumns = ({ onEdit, onDelete }) => {
  return [
    {
      title: '用户名称',
      dataIndex: 'name',
    },
    {
      title: '帐号角色',
      dataIndex: 'accountType',
      render: text => (text === 0 ? '系统管理员' : '普通用户'),
    },
    {
      title: '创建人',
      dataIndex: 'createBy',
    },
    {
      title: '创建时间',
      dataIndex: 'createTime',
      render: text => text && timestampFormat(text),
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
          <Button type="link" onClick={() => onEdit(record)}>
            编辑
          </Button>
          <Button type="link" onClick={() => onDelete(record)}>
            删除
          </Button>
        </>
      ),
    },
  ];
};
