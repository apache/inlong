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
import { SortOrder } from 'antd/es/table/interface';
import { range } from 'lodash';
import { CSVLink } from 'react-csv';
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

export const sumSubValue = sourceDataMap => {
  if (sourceDataMap === null || sourceDataMap === undefined) {
    return 0;
  }
  return Object.keys(sourceDataMap).reduce((acc, cur) => {
    const element = sourceDataMap[cur];
    acc += element.subValue;
    return acc;
  }, 0);
};
export const toTableData = (source, sourceDataMap) => {
  if (sourceDataMap === null || sourceDataMap === undefined) {
    return [];
  }
  return Object.keys(sourceDataMap)
    .reverse()
    .map(logTs => ({
      ...sourceDataMap[logTs],
      logTs,
    }));
};

export const getFormContent = (
  initialValues,
  onSearch,
  auditData,
  sourceData,
  csvData,
  setInlongGroupId,
  setInlongStreamID,
  fileName,
) => [
  {
    type: 'select',
    label: i18n.t('pages.ModuleAudit.config.InlongGroupId'),
    name: 'inlongGroupId',
    props: {
      dropdownMatchSelectWidth: false,
      showSearch: true,
      onChange: (value, option) => {
        setInlongGroupId(value);
      },
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
      onChange: (value, option) => {
        setInlongStreamID(value);
      },
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
      format: 'YYYY-MM-DD HH:mm',
      disabledTime: (date: dayjs.Dayjs, type, info: { from?: dayjs.Dayjs }) => {
        return {
          disabledSeconds: () => range(0, 60),
        };
      },
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
      format: 'YYYY-MM-DD HH:mm',
      disabledTime: (date: dayjs.Dayjs, type, info: { from?: dayjs.Dayjs }) => {
        return {
          disabledSeconds: () => range(0, 60),
        };
      },
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
      options: auditData?.reduce((accumulator, item) => {
        const existingItem = accumulator.find((i: { value: any }) => i.value === item.auditId);
        if (!existingItem) {
          accumulator.push({
            label: i18n?.language === 'cn' ? item.nameInChinese : item.nameInEnglish,
            value: item.auditId,
          });
        }
        return accumulator;
      }, []),
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
      options: auditData?.reduce((accumulator, item) => {
        const existingItem = accumulator.find((i: { value: any }) => i.value === item.auditId);
        if (!existingItem) {
          accumulator.push({
            label: i18n?.language === 'cn' ? item.nameInChinese : item.nameInEnglish,
            value: item.auditId,
          });
        }
        return accumulator;
      }, []),
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
  {
    type: (
      <Button type="primary" disabled={!(sourceData.length > 0)}>
        <CSVLink data={csvData} filename={fileName}>
          {i18n.t('pages.GroupDetail.Audit.ExportCSV')}
        </CSVLink>
      </Button>
    ),
  },
];

const strSorter = (a, b) => {
  return a?.ip.localeCompare(b?.ip);
};
const sortOrder: SortOrder = 'descend';

const baseSorter = (a, b) => {
  return a.base - b.base;
};
const comparedSorter = (a, b) => {
  return a.compared - b.compared;
};
const subValueSorter = (a, b) => {
  return a.subValue - b.subValue;
};

export const getTableColumns = (source: any) => {
  const data = source.map(item => ({
    title: item.auditName,
    dataIndex: source[0].auditId === item.auditId ? 'base' : 'compared',
    key: source[0].auditId === item.auditId ? 'base' : 'compared',
    sorter: {
      compare: source[0].auditId === item.auditId ? baseSorter : comparedSorter,
      multiple: source[0].auditId === item.auditId ? 3 : 4,
    },
    render: text => text || 0,
  }));
  return [
    {
      title: i18n.t('pages.ModuleAudit.config.Ip'),
      dataIndex: 'ip',
      defaultSortOrder: sortOrder,
      sorter: strSorter,
    },
  ]
    .concat(data)
    .concat({
      title: i18n.t('pages.ModuleAudit.config.SubValue'),
      dataIndex: 'subValue',
      defaultSortOrder: null,
      sorter: subValueSorter,
    });
};
