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

import i18n from '@/i18n';
import { Tooltip } from 'antd';
import React from 'react';
import { dateFormat } from '@/core/utils';

const targetList = [
  {
    label: 'Group',
    value: 'GROUP',
  },
  {
    label: 'Stream',
    value: 'STREAM',
  },
  {
    label: 'Source',
    value: 'SOURCE',
  },
  {
    label: 'Sink',
    value: 'SINK',
  },
];

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

export const getFormContent = inlongGroupId => [
  {
    type: 'select',
    label: i18n.t('pages.GroupDetail.OperationLog.Stream'),
    name: 'inlongStreamId',
    props: {
      options: {
        requestAuto: true,
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/stream/list',
          method: 'POST',
          data: {
            keyword,
            pageNum: 1,
            pageSize: 20,
            inlongGroupId,
          },
        }),
        requestParams: {
          formatResult: result =>
            result?.list?.map(item => ({
              label: item.inlongStreamId,
              value: item.inlongStreamId,
            })),
        },
      },
      allowClear: true,
      showSearch: true,
      filterOption: (keyword: string, option: { label: any }) => {
        return (option?.label ?? '').toLowerCase().includes(keyword.toLowerCase());
      },
    },
  },
  {
    type: 'select',
    label: i18n.t('pages.GroupDetail.OperationLog.OperationTarget'),
    name: 'operationTarget',
    props: {
      dropdownMatchSelectWidth: false,
      options: targetList,
      allowClear: true,
      showSearch: true,
      filterOption: (keyword: string, option: { label: any }) => {
        return (option?.label ?? '').toLowerCase().includes(keyword.toLowerCase());
      },
    },
  },
  {
    type: 'select',
    label: i18n.t('pages.GroupDetail.OperationLog.OperationType'),
    name: 'operationType',
    props: {
      dropdownMatchSelectWidth: false,
      options: typeList,
      allowClear: true,
      showSearch: true,
      filterOption: (keyword: string, option: { label: any }) => {
        return (option?.label ?? '').toLowerCase().includes(keyword.toLowerCase());
      },
    },
  },
];

export const getTableColumns = [
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.GroupId'),
    dataIndex: 'inlongGroupId',
  },
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.StreamId'),
    dataIndex: 'inlongStreamId',
  },
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
    title: i18n.t('pages.GroupDetail.OperationLog.Table.OperationTarget'),
    dataIndex: 'operationTarget',
    render: text => targetList.find(c => c.value === text)?.label || text,
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
