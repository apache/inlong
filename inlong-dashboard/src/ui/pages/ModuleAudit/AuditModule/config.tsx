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
import { sinks } from '@/plugins/sinks';
import request from '@/core/utils/request';

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

const auditList = ['Agent', 'DataProxy', 'Sort'].reduce((acc, item, index) => {
  return acc.concat([
    {
      label: `${item} ${i18n.t('pages.GroupDetail.Audit.Receive')}`,
      value: index * 2 + 3,
    },
    {
      label: `${item} ${i18n.t('pages.GroupDetail.Audit.Send')}`,
      value: index * 2 + 4,
    },
  ]);
}, []);

const auditMap = auditList.reduce(
  (acc, cur) => ({
    ...acc,
    [cur.value]: cur,
  }),
  {},
);

function getAuditLabel(auditId: number, nodeType?: string) {
  const id = +auditId;
  const item = id >= 9 ? auditMap[id % 2 ? 7 : 8] : auditMap[id];
  const label = item?.label || id;
  const sinkLabel = sinks.find(c => c.value === nodeType)?.label;
  return nodeType ? `${label}(${sinkLabel})` : label;
}

export const toChartData = (source, sourceDataMap) => {
  console.log(source, sourceDataMap);
  const xAxisData = Object.keys(sourceDataMap ? sourceDataMap : '12345');
  return {
    legend: {
      data: source.map(item => item?.auditName),
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
      name: item?.auditName ? item?.auditName : '1233',
      type: 'line',
      data: xAxisData.map(logTs => sourceDataMap[logTs]?.[item?.auditId] || 0),
    })),
  };
};

export const toTableData = (source, sourceDataMap) => {
  const map = Object.keys(sourceDataMap)
    .reverse()
    .map(logTs => ({
      ...sourceDataMap[logTs],
      logTs,
    }));
  let sourceData = getSourceDataWithPercent(source, map);
  return getSourceDataWithCommas(sourceData);
};

export const getSourceDataWithPercent = (sourceKeys, sourceMap) => {
  const auditIds = Array.from(
    new Set(Object.values(sourceKeys).map(({ auditId }) => parseInt(auditId))),
  );
  return sourceMap.map(source => {
    for (const auditId of auditIds) {
      if (!(auditId in source)) {
        source[auditId] = 0;
      }
    }
    let newSource = {};
    const keys = Object.keys(source).filter(key => key !== 'logTs');
    const firstKey = keys[0];
    const firstValue = source[firstKey];
    newSource[firstKey] = firstValue.toString();
    for (let key of keys.slice(1)) {
      if (key !== 'logTs') {
        let diff = getDiff(firstValue, source[key]);
        newSource[key] = `${source[key]} (${diff})`;
      }
    }
    newSource['logTs'] = source['logTs'];
    return newSource;
  });
};

export const getDiff = (first, current) => {
  if (first === 0) {
    return '0%';
  }
  let result;
  const diff = Math.ceil((current / first - 1) * 100);
  result = diff > 0 ? '+' + diff + '%' : diff + '%';
  return result;
};

export const getSourceDataWithCommas = sourceData => {
  sourceData.map(source => {
    for (const key in source) {
      if (key !== 'logTs') {
        let parts = source[key].split(' ');
        let numberPart = parts[0];
        let percentPart = parts[1] || '';
        let number = parseInt(numberPart, 10);
        let formattedNumber = number.toLocaleString();
        source[key] = formattedNumber + ' ' + percentPart;
      }
    }
  });
  return sourceData;
};

let endTimeVisible = true;

export const getFormContent = (initialValues, onSearch, onDataStreamSuccess, auditData) => [
  {
    type: 'select',
    label: i18n.t('pages.ModuleAudit.config.InlongGroupId'),
    name: 'inlongGroupId',
    props: values => ({
      dropdownMatchSelectWidth: false,
      showSearch: true,
      allowClear: true,
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
    }),
    rules: [{ required: true }],
  },
  {
    type: 'select',
    label: i18n.t('pages.ModuleAudit.config.InlongStreamId'),
    name: 'inlongStreamId',
    props: values => ({
      dropdownMatchSelectWidth: false,
      showSearch: true,
      allowClear: true,
      disabled: !Boolean(values.inlongGroupId),
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
    rules: [{ required: true }],
  },

  {
    type: 'datepicker',
    label: i18n.t('pages.GroupDetail.Audit.StartDate'),
    name: 'startDate',
    initialValue: dayjs(initialValues.startDate),
    props: {
      allowClear: false,
      format: 'YYYY-MM-DD',
    },
  },
  {
    type: 'datepicker',
    label: i18n.t('pages.GroupDetail.Audit.EndDate'),
    name: 'endDate',
    initialValue: dayjs(initialValues.endDate),
    rules: [
      { required: true },
      ({ getFieldValue }) => ({
        validator(_, value) {
          const dim = initialValues.timeStaticsDim;
          if (dim === 'MINUTE') {
            return Promise.resolve();
          }
          const timeDiff = value - getFieldValue('startDate');
          console.log('timeDiff', value, getFieldValue('startDate'), timeDiff);
          if (timeDiff >= 0) {
            const isHourDiff = dim === 'HOUR' && timeDiff < 1000 * 60 * 60 * 24 * 3;
            const isDayDiff = dim === 'DAY' && timeDiff < 1000 * 60 * 60 * 24 * 7;
            if (isHourDiff || isDayDiff) {
              return Promise.resolve();
            }
          }
          return Promise.reject(new Error(i18n.t('pages.GroupDetail.Audit.DatepickerRule')));
        },
      }),
    ],
    props: {
      allowClear: false,
      format: 'YYYY-MM-DD',
      disabled: initialValues.timeStaticsDim === 'MINUTE',
      disabledDate: current => {
        const start = dayjs(initialValues.startDate);
        const dim = initialValues.timeStaticsDim;
        const tooEarly = current < start.add(-1, 'd').endOf('day');
        let tooLate;
        if (dim === 'HOUR') {
          tooLate = current >= start.add(2, 'd').endOf('day');
        }
        if (dim === 'DAY') {
          tooLate = current >= start.add(6, 'd').endOf('day');
        }
        return current && (tooLate || tooEarly);
      },
    },
  },
  {
    type: 'select',
    label: i18n.t('pages.GroupDetail.Audit.TimeStaticsDim'),
    name: 'timeStaticsDim',
    initialValue: initialValues.timeStaticsDim,
    props: {
      dropdownMatchSelectWidth: false,
      options: timeStaticsDimList,
    },
  },
  {
    type: 'select',
    label: i18n.t('pages.GroupDetail.Metric.Item'),
    name: 'auditIds',
    rules: [{ required: true }],
    props: {
      style: {
        width: 200,
      },
      mode: 'multiple',
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
      filterOption: (keyword: string, option: { label: any }) => {
        return (option?.label ?? '').toLowerCase().includes(keyword.toLowerCase());
      },
    },
  },
  {
    type: (
      <Button type="primary" onClick={onSearch}>
        {i18n.t('pages.GroupDetail.Audit.Search')}
      </Button>
    ),
  },
];

export const getTableColumns = (source, dim) => {
  const data = source.map(item => ({
    title: item.auditName,
    dataIndex: item.auditId,
    render: text => {
      if (text?.includes('+')) {
        return <span style={{ color: 'red' }}>{text}</span>;
      } else if (text?.includes('-')) {
        return <span style={{ color: 'green' }}>{text}</span>;
      }
      return <span>{text}</span>;
    },
  }));
  return [
    {
      title: i18n.t('pages.GroupDetail.Audit.Time'),
      dataIndex: 'logTs',
      render: text => {
        return dim === 'MINUTE' ? dayjs(text).format('HH:mm') : text;
      },
    },
  ].concat(data);
};
