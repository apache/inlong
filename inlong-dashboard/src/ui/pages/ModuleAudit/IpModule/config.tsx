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
import { CSVLink } from 'react-csv';
import { range } from 'lodash';
import { SortOrder } from 'antd/es/table/interface';
export const toChartData = (source, sourceDataMap) => {
  const xAxisData = Object.keys(sourceDataMap);
  return {
    legend: {
      data: source.map(item => item.auditName),
    },
    tooltip: {
      trigger: 'axis',
    },
    xAxis: {
      type: 'category',
      data: xAxisData,
    },
    yAxis: {
      type: 'value',
    },
    series: source.map(item => ({
      name: item.auditName,
      type: 'line',
      data: xAxisData.map(logTs => sourceDataMap[logTs]?.[item.auditId] || 0),
    })),
  };
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
  setIp,
  fileName,
) => [
  {
    type: 'input',
    label: i18n.t('pages.ModuleAudit.config.Ip'),
    props: {
      onChange: (e: any) => {
        setIp(e.target.value);
      },
    },
    name: 'ip',
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
    initialValues: dayjs().startOf('hour').valueOf(),
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
      filterOption: (keyword: any, option: { label: any }) =>
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
const baseSorter = (a, b) => {
  return a.base - b.base;
};
const comparedSorter = (a, b) => {
  return a.compared - b.compared;
};
const subValueSorter = (a, b) => {
  return a.subValue - b.subValue;
};
const groupIdStrSorter = (a, b) => {
  return a?.inlongGroupId.localeCompare(b.inlongGroupId);
};
const streamIdStrSorter = (a, b) => {
  return a?.inlongStreamId.localeCompare(b?.inlongStreamId);
};
const sortOrder: SortOrder = 'descend';
export const getTableColumns = source => {
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
      title: i18n.t('pages.ModuleAudit.config.InlongGroupId'),
      dataIndex: 'inlongGroupId',
      key: 'inlongGroupId',
      sorter: {
        compare: groupIdStrSorter,
        multiple: 1,
      },
    },
    {
      title: i18n.t('pages.ModuleAudit.config.InlongStreamId'),
      dataIndex: 'inlongStreamId',
      key: 'inlongStreamId',
      defaultSortOrder: sortOrder,
      sorter: {
        compare: streamIdStrSorter,
        multiple: 2,
      },
    },
  ]
    .concat(data)
    .concat([
      {
        title: i18n.t('pages.ModuleAudit.config.SubValue'),
        dataIndex: 'subValue',
        key: 'subValue',
        sorter: {
          compare: subValueSorter,
          multiple: 5,
        },
      },
    ]);
};
