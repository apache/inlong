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
import { CSVLink } from 'react-csv';
import audit from '@/ui/pages/GroupDetail/Audit/index';

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
    const dpValue = source[keys[2]] || 0;
    newSource[firstKey] = firstValue.toString();
    for (let key of keys.slice(1)) {
      let diff = '0%';
      if (firstValue === 0 && dpValue === 0) {
        diff = firstValue.toFixed(4) + '%';
      }
      if ((key === '4' || key === '5') && firstValue === 0) {
        diff = firstValue.toFixed(4) + '%';
      } else {
        diff = getDiff(firstValue, dpValue, source[key]);
      }
      newSource[key] = `${source[key]} (${diff})`;
    }
    newSource['logTs'] = source['logTs'];
    return newSource;
  });
};

export const getDiff = (first, dpValue, current) => {
  let result;
  const diff = (current / (first === 0 ? dpValue : first) - 1) * 100;
  result = diff > 0 ? '+' + diff.toFixed(4) + '%' : diff.toFixed(4) + '%';
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

const getCommonFormContent = (initialValues, onSearch, sourceData, csvData, fileName): any[] => {
  return [
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
            console.log(timeDiff);
            if (timeDiff >= 0) {
              const isHourDiff = dim === 'HOUR' && timeDiff < 1000 * 60 * 60 * 24 * 2;
              const isDayDiff = dim === 'DAY' && timeDiff < 1000 * 60 * 60 * 24 * 90;
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
            tooLate = current >= start.add(90, 'd').endOf('day');
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
      label: i18n.t('pages.GroupDetail.Audit.Item'),
      name: 'auditIds',
      props: {
        style: {
          width: 200,
        },
        mode: 'multiple',
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
            formatResult: result => {
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
};
export const getFormContent = (
  inlongGroupId,
  initialValues,
  onSearch,
  onDataStreamSuccess,
  sourceData,
  csvData,
  fileName,
  setInlongStreamID,
  inlongStreamId,
  key,
) => {
  const commonFormContent = getCommonFormContent(
    initialValues,
    onSearch,
    sourceData,
    csvData,
    fileName,
  );
  if (key === 'stream') {
    return [
      {
        type: 'select',
        label: i18n.t('pages.ModuleAudit.config.InlongStreamId'),
        name: 'inlongStreamId',
        initialValue: inlongStreamId,
        props: {
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
                inlongGroupId,
              },
            }),
            requestParams: {
              formatResult: result =>
                result?.list.map(item => ({
                  label: item.inlongStreamId,
                  value: item.inlongStreamId,
                })) || [],
              onSuccess: onDataStreamSuccess,
            },
          },
        },
        rules: [{ required: true }],
      },
      {
        type: 'select',
        label: i18n.t('pages.GroupDetail.Audit.Sink'),
        name: 'sinkId',
        initialValue: initialValues.sinkId,
        props: values => ({
          dropdownMatchSelectWidth: false,
          showSearch: true,
          options: {
            requestTrigger: ['onOpen', 'onSearch'],
            requestService: keyword => ({
              url: '/sink/list',
              method: 'POST',
              data: {
                keyword,
                pageNum: 1,
                pageSize: 100,
                inlongGroupId,
                inlongStreamId: inlongStreamId,
              },
            }),
            requestParams: {
              formatResult: result =>
                result?.list.map(item => ({
                  label:
                    item.sinkName + ` ( ${sinks.find(c => c.value === item.sinkType)?.label} )`,
                  value: item.id,
                })) || [],
            },
          },
          filterOption: (keyword: string, option: { label: any }) => {
            return (option?.label ?? '').toLowerCase().includes(keyword.toLowerCase());
          },
        }),
      },
    ].concat(commonFormContent);
  } else {
    const sinkTypeList = sinks
      .filter(item => item.value !== '')
      .map(item => {
        return {
          label: item.label,
          value: item.value,
        };
      });
    return [
      {
        type: 'input',
        label: i18n.t('meta.Group.InlongGroupId'),
        name: 'inlongGroupId',
        initialValue: inlongGroupId,
        props: {
          disabled: true,
          style: {
            width: 200,
          },
        },
        rules: [{ required: true }],
      },
      {
        type: 'select',
        label: i18n.t('pages.GroupDetail.SinkType'),
        name: 'sinkType',
        initialValue: initialValues.sinkType,
        props: {
          allowClear: true,
          dropdownMatchSelectWidth: false,
          showSearch: true,
          options: sinkTypeList,
        },
      },
    ].concat(commonFormContent);
  }
};

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
