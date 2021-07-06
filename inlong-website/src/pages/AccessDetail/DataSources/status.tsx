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
import StatusTag, { StatusTagProps } from '@/components/StatusTag';
import { ClockCircleFilled } from '@/components/Icons';

type StatusProp = {
  label: string;
  value: string | number;
  type: StatusTagProps['type'];
  icon?: StatusTagProps['icon'];
};

export const statusList: StatusProp[] = [
  {
    label: '草稿',
    value: 0,
    type: 'default',
  },
  {
    label: '新建',
    value: 200,
    type: 'primary',
    icon: <ClockCircleFilled />,
  },
  {
    label: '已删除',
    value: 201,
    type: 'error',
  },
  {
    label: '下发失败',
    value: 61,
    type: 'error',
  },
  {
    label: '配置失败',
    value: 42,
    type: 'error',
  },
  {
    label: '启动失败',
    value: 52,
    type: 'error',
  },
  {
    label: '正常',
    value: 11,
    type: 'success',
  },
  {
    label: '移除配置',
    value: 44,
    type: 'warning',
  },
  {
    label: '不可用',
    value: 15,
    type: 'error',
  },
];

export const statusMap = statusList.reduce(
  (acc, cur) => ({
    ...acc,
    [cur.value]: cur,
  }),
  {},
);

export const genStatusTag = (value: StatusProp['value']) => {
  const item = statusMap[value] || {};

  return <StatusTag type={item.type || 'default'} title={item.label || value} icon={item.icon} />;
};
