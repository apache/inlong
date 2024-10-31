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

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import HighTable from '@/ui/components/HighTable';
import { useRequest } from '@/ui/hooks';
import { timestampFormat } from '@/core/utils';
import Charts from '@/ui/components/Charts';
import {
  getFormContent,
  toChartData,
  toTableData,
  getTableColumns,
  timeStaticsDimList,
} from './config';
import { Table } from 'antd';
import i18n from '@/i18n';
import { AuditProps } from '@/ui/pages/ModuleAudit';

export const auditModule = 'audit';
const Comp: React.FC<AuditProps> = ({ auditData }) => {
  const [form] = useForm();

  const [query, setQuery] = useState({
    inlongGroupId: '',
    inlongStreamId: '',
    startDate: +new Date(),
    endDate: +new Date(),
    timeStaticsDim: timeStaticsDimList[0].value,
  });
  const [inlongStreamID, setInlongStreamID] = useState('');
  const [inlongGroupId, setInlongGroupId] = useState('');
  const { data: sourceData = [], run } = useRequest(
    {
      url: '/audit/list',
      method: 'POST',
      data: {
        ...query,
        startDate: timestampFormat(query.startDate, 'yyyy-MM-dd'),
        endDate: timestampFormat(query.endDate, 'yyyy-MM-dd'),
      },
    },
    {
      ready: Boolean(query.inlongStreamId),
      formatResult: result => result.sort((a, b) => (a.auditId - b.auditId > 0 ? 1 : -1)),
    },
  );
  const numToName = useCallback(
    num => {
      let obj = {};
      sourceData.forEach(item => {
        obj = { ...obj, [item.auditId]: item.auditName };
      });
      obj = { ...obj, logTs: i18n.t('pages.GroupDetail.Audit.Time') };
      return obj[num];
    },
    [sourceData],
  );
  const sourceDataMap = useMemo(() => {
    const flatArr = sourceData
      .reduce(
        (acc, cur) =>
          acc.concat(
            cur.auditSet.map(item => ({
              ...item,
              auditId: cur.auditId,
            })),
          ),
        [],
      )
      .sort((a, b) => {
        const aT = +new Date(query.timeStaticsDim === 'HOUR' ? `${a.logTs}:00` : a.logTs);
        const bT = +new Date(query.timeStaticsDim === 'HOUR' ? `${b.logTs}:00` : b.logTs);
        return aT - bT;
      });
    const output = flatArr.reduce((acc, cur) => {
      if (!acc[cur.logTs]) {
        acc[cur.logTs] = {};
      }
      acc[cur.logTs] = {
        ...acc[cur.logTs],
        [cur.auditId]: cur.count,
      };
      return acc;
    }, {});
    return output;
  }, [sourceData, query.timeStaticsDim]);

  const onSearch = async () => {
    let values = await form.validateFields();
    if (values.timeStaticsDim == 'MINUTE') {
      setQuery(prev => ({ ...prev, endDate: prev.startDate }));
    }
    run();
  };
  const metricSum = useMemo(() => {
    let obj = { logTs: i18n.t('pages.GroupDetail.Audit.Total') };
    sourceData.map(item => {
      const sum = item.auditSet?.reduce((total, cur) => {
        return total + cur.count;
      }, 0);
      obj = { ...obj, [item.auditId]: sum };
    });
    return obj;
  }, [sourceData]);

  const csvData = useMemo(() => {
    const result = [...toTableData(sourceData, sourceDataMap), metricSum].map(item => {
      let obj = {};
      Object.keys(item)
        .reverse()
        .forEach(key => {
          obj = { ...obj, [numToName(key)]: item[key] };
        });
      return obj;
    });
    return result;
  }, [sourceData, sourceDataMap, metricSum]);
  const onDataStreamSuccess = data => {
    const defaultDataStream = data[0]?.value;
    if (defaultDataStream) {
      form.setFieldsValue({ inlongStreamId: defaultDataStream });
      setQuery(prev => ({ ...prev, inlongStreamId: defaultDataStream }));
      run();
    }
  };
  const [fileName, setFileName] = useState('metrics.csv');
  useEffect(() => {
    setFileName(`metrics_${inlongGroupId}_${inlongStreamID}.csv`);
  }, [inlongGroupId, inlongStreamID]);
  return (
    <>
      <div style={{ marginBottom: 40 }}>
        <FormGenerator
          form={form}
          layout="inline"
          content={getFormContent(
            query,
            onSearch,
            onDataStreamSuccess,
            auditData,
            sourceData,
            csvData,
            setInlongGroupId,
            setInlongStreamID,
            fileName,
          )}
          style={{ marginBottom: 30, gap: 10 }}
          onFilter={allValues =>
            setQuery({
              ...allValues,
              startDate: +allValues.startDate.$d,
              endDate: +allValues.endDate.$d,
            })
          }
        />
        <Charts height={400} option={toChartData(sourceData, sourceDataMap)} forceUpdate={true} />
      </div>

      <HighTable
        table={{
          columns: getTableColumns(sourceData, query.timeStaticsDim),
          dataSource: toTableData(sourceData, sourceDataMap),
          rowKey: 'logTs',
          summary: () => (
            <Table.Summary fixed>
              <Table.Summary.Row>
                <Table.Summary.Cell index={0}>
                  {i18n.t('pages.GroupDetail.Audit.Total')}
                </Table.Summary.Cell>
                {sourceData.map((row, index) => (
                  <Table.Summary.Cell key={index} index={index + 1}>
                    {row.auditSet.reduce((total, item) => total + item.count, 0).toLocaleString()}
                  </Table.Summary.Cell>
                ))}
              </Table.Summary.Row>
            </Table.Summary>
          ),
        }}
      />
    </>
  );
};

export default Comp;
