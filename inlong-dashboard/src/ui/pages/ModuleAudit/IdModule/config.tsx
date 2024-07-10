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

import dayjs from 'dayjs';
import i18n from '@/i18n';
import request from '@/core/utils/request';
import { Button } from 'antd';
import React from 'react';

export const timeStaticsDimList = [
  {
    label: i18n.t('pages.GroupDetail.Audit.Min'),
    value: 'MINUTE',
  },
  {
    label: i18n.t('pages.GroupDetail.Audit.Hour'),
    value: 'HOUR',
  },
  {
    label: i18n.t('pages.GroupDetail.Audit.Day'),
    value: 'DAY',
  },
];

export const toTableData = (source, sourceDataMap) => {
  return Object.keys(sourceDataMap)
    .reverse()
    .map(logTs => ({
      ...sourceDataMap[logTs],
      logTs,
    }));
};

export const getFormContent = (initialValues, onSearch) => [
  {
    type: 'select',
    label: i18n.t('pages.ModuleAudit.config.InlongGroupId'),
    name: 'inlongGroupId',
    props: {
      dropdownMatchSelectWidth: false,
      showSearch: true,
      options: {
        requestAuto: true,
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/group/list',
          method: 'POST',
          data: {
            keyword,
            pageNum: 1,
            pageSize: 100,
            inlongGroupMode: 0,
          },
        }),
        requestParams: {
          formatResult: result =>
            result?.list.map(item => ({
              label: item.inlongGroupId,
              value: item.inlongGroupId,
            })) || [],
        },
      },
    },
  },
  {
    type: 'select',
    label: i18n.t('pages.ModuleAudit.config.InlongStreamId'),
    name: 'inlongStreamId',
    props: values => ({
      dropdownMatchSelectWidth: false,
      showSearch: true,
      options: {
        requestAuto: true,
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/stream/list',
          method: 'POST',
          data: {
            keyword,
            pageNum: 1,
            pageSize: 100,
            inlongGroupId: values.inlongGroupId,
          },
        }),
        requestParams: {
          formatResult: result =>
            result?.list.map(item => ({
              label: item.inlongStreamId,
              value: item.inlongStreamId,
            })) || [],
        },
      },
    }),
  },
  {
    type: 'datepicker',
    label: i18n.t('pages.GroupDetail.Audit.StartDate'),
    name: 'startDate',
    initialValue: dayjs(initialValues.startDate),
    props: {
      allowClear: false,
      showTime: true,
      format: 'YYYY-MM-DD HH:mm:ss',
    },
  },
  {
    type: 'datepicker',
    label: i18n.t('pages.GroupDetail.Audit.EndDate'),
    name: 'endDate',
    initialValues: dayjs(initialValues.endDate),
    props: {
      allowClear: false,
      showTime: true,
      format: 'YYYY-MM-DD HH:mm:ss',
    },
  },
  {
    type: 'select',
    label: i18n.t('pages.ModuleAudit.config.BenchmarkIndicator'),
    name: 'benchmark',
    props: {
      allowClear: true,
      showSearch: true,
      dropdownMatchSelectWidth: false,
      options: {
        requestAuto: true,
        requestTrigger: ['onOpen'],
        requestService: () => {
          return request('/audit/getAuditBases');
        },
        requestParams: {
          formatResult: (result: any[]) => {
            return result?.reduce((accumulator, item) => {
              const existingItem = accumulator.find(
                (i: { value: any }) => i.value === item.auditId,
              );
              if (!existingItem) {
                accumulator.push({
                  label: i18n?.language === 'cn' ? item.nameInChinese : item.nameInEnglish,
                  value: item.auditId,
                });
              }
              return accumulator;
            }, []);
          },
        },
      },
      filterOption: (keyword: string, option: { label: any }) =>
        (option?.label ?? '').toLowerCase().includes(keyword.toLowerCase()),
    },
  },
  {
    type: 'select',
    label: i18n.t('pages.ModuleAudit.config.ComparativeIndicators'),
    name: 'compared',
    props: {
      allowClear: true,
      showSearch: true,
      dropdownMatchSelectWidth: false,
      options: {
        requestAuto: true,
        requestTrigger: ['onOpen'],
        requestService: () => {
          return request('/audit/getAuditBases');
        },
        requestParams: {
          formatResult: (result: any[]) => {
            return result?.reduce((accumulator, item) => {
              const existingItem = accumulator.find(
                (i: { value: any }) => i.value === item.auditId,
              );
              if (!existingItem) {
                accumulator.push({
                  label: i18n?.language === 'cn' ? item.nameInChinese : item.nameInEnglish,
                  value: item.auditId,
                });
              }
              return accumulator;
            }, []);
          },
        },
      },
      filterOption: (keyword: string, option: { label: any }) =>
        (option?.label ?? '').toLowerCase().includes(keyword.toLowerCase()),
    },
  },
  {
    type: (
      <Button type="primary" onClick={onSearch}>
        {i18n.t('basic.Refresh')}
      </Button>
    ),
  },
];

export const getTableColumns = source => {
  const data = source.map(item => ({
    title: item.auditName,
    dataIndex: item.auditId,
    render: text => text || 0,
  }));
  return [
    {
      title: i18n.t('pages.ModuleAudit.config.Ip'),
      dataIndex: 'ip',
    },
  ].concat(data);
};
