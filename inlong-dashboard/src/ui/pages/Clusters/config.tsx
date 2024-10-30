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

import request from '@/core/utils/request';
import i18n from '@/i18n';
import { Tooltip } from 'antd';
import { dateFormat } from '@/core/utils';
import React from 'react';

export const getModuleList = async () => {
  return await request({
    url: '/module/list',
    method: 'POST',
    data: {
      pageNum: 1,
      pageSize: 9999,
    },
  });
};

export const versionMap = data => {
  return data
    ?.filter(item => item.type === 'AGENT')
    .map(item => ({
      ...item,
      label: `${item.name} ${item.version}`,
      value: item.id,
    }))
    .reduce(
      (acc, cur) => ({
        ...acc,
        [cur.value]: cur.label,
      }),
      {},
    );
};

export const installerMap = () => {
  return getModuleList().then(res =>
    res?.list
      ?.filter(item => item.type === 'INSTALLER')
      .map(item => ({
        ...item,
        label: `${item.name} ${item.version}`,
        value: item.id,
      }))
      .reduce(
        (acc, cur) => ({
          ...acc,
          [cur.value]: cur.label,
        }),
        {},
      ),
  );
};

const typeList = [
  {
    label: 'Create',
    value: 'CREATE',
  },
  {
    label: 'Update',
    value: 'UPDATE',
  },
  {
    label: 'Delete',
    value: 'DELETE',
  },
  {
    label: 'Get',
    value: 'GET',
  },
];

export const getFormContent = () => [
  {
    type: 'select',
    label: i18n.t('pages.GroupDetail.OperationLog.OperationType'),
    name: 'operationType',
    props: {
      allowClear: true,
      dropdownMatchSelectWidth: false,
      options: typeList,
    },
  },
];

export const getTableColumns = [
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.Operator'),
    dataIndex: 'operator',
  },
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.OperationType'),
    dataIndex: 'operationType',
    render: text => typeList.find(c => c.value === text)?.label || text,
  },
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.Log'),
    dataIndex: 'body',
    ellipsis: true,
    render: body => (
      <Tooltip placement="topLeft" title={body}>
        {body}
      </Tooltip>
    ),
  },
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.OperationTime'),
    dataIndex: 'requestTime',
    render: text => dateFormat(new Date(text)),
  },
];
