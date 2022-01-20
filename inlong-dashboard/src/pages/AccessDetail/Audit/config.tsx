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
import dayjs from 'dayjs';
import i18n from '@/i18n';

export const auditList = ['InLong Api', 'InLong Agent', 'InLong DataProxy', 'Inlong Sort'].reduce(
  (acc, item, index) => {
    return acc.concat([
      {
        label: `${item} ${i18n.t('pages.AccessDetail.Audit.Receive')}`,
        value: index * 2,
      },
      {
        label: `${item} ${i18n.t('pages.AccessDetail.Audit.Send')}`,
        value: index * 2 + 1,
      },
    ]);
  },
  [],
);

const auditMap = auditList.reduce(
  (acc, cur) => ({
    ...acc,
    [cur.value]: cur,
  }),
  {},
);

export const toChartData = source => {
  return {
    legend: {
      data: source.map(item => auditMap[item.auditId]?.label),
    },
    tooltip: {
      trigger: 'axis',
    },
    xAxis: {
      type: 'category',
      data: source[0]?.auditSet?.map(item => item.logTs),
    },
    yAxis: {
      type: 'value',
    },
    series: source.map(item => ({
      name: auditMap[item.auditId]?.label,
      type: 'line',
      data: item.auditSet.map(k => k.count),
    })),
  };
};

export const toTableData = source => {
  const flatArr = source.reduce(
    (acc, cur) =>
      acc.concat(
        cur.auditSet.map(item => ({
          ...item,
          auditId: cur.auditId,
        })),
      ),
    [],
  );
  const objData = flatArr.reduce((acc, cur) => {
    if (!acc[cur.logTs]) {
      acc[cur.logTs] = {};
    }
    acc[cur.logTs] = {
      ...acc[cur.logTs],
      [cur.auditId]: cur.count,
    };
    return acc;
  }, {});
  return Object.keys(objData).map(logTs => ({
    ...objData[logTs],
    logTs,
  }));
};

export const getFormContent = (inlongGroupId, initialValues, onSearch) => [
  {
    type: 'select',
    label: i18n.t('pages.AccessDetail.Audit.DataStream'),
    name: 'inlongStreamId',
    props: {
      options: {
        requestService: {
          url: '/datastream/list',
          params: {
            pageNum: 1,
            pageSize: 1000,
            inlongGroupId,
          },
        },
        requestParams: {
          formatResult: result =>
            result?.list.map(item => ({
              label: item.inlongStreamId,
              value: item.inlongStreamId,
            })) || [],
        },
      },
    },
    rules: [{ required: true }],
  },
  {
    type: 'datepicker',
    label: i18n.t('pages.AccessDetail.Audit.Date'),
    name: 'dt',
    initialValue: dayjs(initialValues.dt),
    props: {
      allowClear: false,
      format: 'YYYY-MM-DD',
    },
  },
  {
    type: 'select',
    label: i18n.t('pages.AccessDetail.Audit.AuditIds'),
    name: 'auditIds',
    initialValue: initialValues.auditIds,
    props: {
      mode: 'multiple',
      dropdownMatchSelectWidth: false,
      options: auditList,
    },
    rules: [{ required: true }],
  },
  {
    type: (
      <Button type="primary" onClick={onSearch}>
        {i18n.t('pages.AccessDetail.Audit.Search')}
      </Button>
    ),
  },
];

export const getTableColumns = source => {
  const data = source.map(item => ({
    title: auditMap[item.auditId]?.label || item.auditId,
    dataIndex: item.auditId,
  }));
  return [
    {
      title: i18n.t('pages.AccessDetail.Audit.Time'),
      dataIndex: 'logTs',
    },
  ].concat(data);
};
