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
import { CommonInterface } from '../common';
import {
  getFormContent,
  toChartData,
  toTableData,
  getTableColumns,
  timeStaticsDimList,
} from './config';
import { Table, Radio, Tabs, TabsProps } from 'antd';
import i18n from '@/i18n';
import './index.less';
import { startCase } from 'lodash';
type Props = CommonInterface;
const initialQuery = {
  inlongStreamId: null,
  startDate: +new Date(),
  endDate: +new Date(),
  timeStaticsDim: timeStaticsDimList[0].value,
  sinkId: null,
  sinkType: null,
};
const Comp: React.FC<Props> = ({ inlongGroupId }) => {
  const [form] = useForm();
  const [query, setQuery] = useState(initialQuery);
  const [inlongStreamID, setInlongStreamID] = useState(null);
  const [type, setType] = useState('count');
  const [subTab, setSubTab] = useState('stream');

  const onDataStreamSuccess = data => {
    const defaultDataStream = data[0]?.value;
    if (defaultDataStream) {
      setInlongStreamID(defaultDataStream);
      form.setFieldsValue({ inlongStreamId: defaultDataStream });
      setQuery(prev => ({ ...prev, inlongStreamId: defaultDataStream }));
    }
  };

  const { data: streamList } = useRequest(
    {
      url: '/stream/list',
      method: 'POST',
      data: {
        pageNum: 1,
        pageSize: 9999,
        inlongGroupId,
      },
    },
    {
      refreshDeps: [inlongGroupId],
      formatResult: result =>
        result?.list.map(item => ({
          label: item.inlongStreamId,
          value: item.inlongStreamId,
        })) || [],
      onSuccess: onDataStreamSuccess,
    },
  );

  const { data: sourceData = [], run } = useRequest(
    {
      url: '/audit/list',
      method: 'POST',
      data: {
        ...query,
        startDate: timestampFormat(query.startDate, 'yyyy-MM-dd'),
        endDate: timestampFormat(query.endDate, 'yyyy-MM-dd'),
        inlongGroupId,
        inlongStreamId: inlongStreamID,
      },
    },
    {
      ready: !!(subTab === 'group' || (subTab === 'stream' && inlongStreamID)),
      refreshDeps: [
        query.sinkId,
        query.sinkType,
        query.endDate,
        query.startDate,
        query.timeStaticsDim,
        inlongStreamID,
      ],
      formatResult: result => result.sort((a, b) => (a.auditId - b.auditId > 0 ? 1 : -1)),
    },
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
    let output: any;
    if (type === 'count') {
      output = flatArr.reduce((acc, cur) => {
        if (!acc[cur.logTs]) {
          acc[cur.logTs] = {};
        }
        acc[cur.logTs] = {
          ...acc[cur.logTs],
          [cur.auditId]: cur.count,
        };
        return acc;
      }, {});
    } else {
      output = flatArr.reduce((acc, cur) => {
        if (!acc[cur.logTs]) {
          acc[cur.logTs] = {};
        }
        acc[cur.logTs] = {
          ...acc[cur.logTs],
          [cur.auditId]: cur.size,
        };
        return acc;
      }, {});
    }
    return output;
  }, [sourceData, query.timeStaticsDim, type]);

  const onSearch = async () => {
    let values = await form.validateFields();
    if (values.timeStaticsDim == 'MINUTE') {
      setQuery(prev => ({ ...prev, endDate: prev.startDate }));
    } else {
      setQuery(values);
    }
    run();
  };

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
  const [fileName, setFileName] = useState('audit.csv');
  useEffect(() => {
    setFileName(`audit_${inlongGroupId}_${inlongStreamID}.csv`);
    form.setFieldsValue({ sinkId: '' });
  }, [inlongGroupId, inlongStreamID]);

  const onChange = e => {
    const tmp = { ...query };
    if (e.target.value === 'group') {
      tmp.inlongStreamId = null;
      tmp.sinkId = null;
      setInlongStreamID(undefined);
    } else {
      tmp.sinkType = null;
      tmp.inlongStreamId = streamList?.[0]?.value;
      setInlongStreamID(streamList?.[0]?.value);
    }
    setQuery(tmp);
    setSubTab(e.target.value);
  };

  return (
    <>
      <div style={{ marginBottom: 20 }}>
        <Radio.Group defaultValue={subTab} buttonStyle="solid" onChange={e => onChange(e)}>
          <Radio.Button value="group">{i18n.t('pages.GroupDetail.Audit.Group')}</Radio.Button>
          <Radio.Button value="stream">{i18n.t('pages.GroupDetail.Audit.Stream')}</Radio.Button>
        </Radio.Group>
      </div>
      <div style={{ marginBottom: 40 }}>
        <FormGenerator
          form={form}
          layout="inline"
          content={getFormContent(
            inlongGroupId,
            query,
            onSearch,
            streamList,
            sourceData,
            csvData,
            fileName,
            setInlongStreamID,
            inlongStreamID,
            subTab,
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
      </div>
      <div className="audit-container">
        <div className="chart-type">
          <Radio.Group
            defaultValue="count"
            buttonStyle="solid"
            onChange={e => setType(e.target.value)}
          >
            <Radio.Button value="size">{i18n.t('pages.GroupDetail.Audit.Size')}</Radio.Button>
            <Radio.Button value="count">{i18n.t('pages.GroupDetail.Audit.Count')}</Radio.Button>
          </Radio.Group>
        </div>
        <div className="chart-container">
          <Charts height={400} option={toChartData(sourceData, sourceDataMap)} forceUpdate={true} />
        </div>
        <div className="table-container">
          <HighTable
            table={{
              columns: getTableColumns(sourceData, query.timeStaticsDim),
              dataSource: toTableData(sourceData, sourceDataMap),
              rowKey: 'logTs',
              pagination: {
                pageSizeOptions: ['10', '20', '50', '60', '100', '120'],
              },
              summary: () => (
                <Table.Summary fixed>
                  <Table.Summary.Row>
                    <Table.Summary.Cell index={0}>
                      {i18n.t('pages.GroupDetail.Audit.Total')}
                    </Table.Summary.Cell>
                    {sourceData.map((row, index) => (
                      <Table.Summary.Cell index={index + 1}>
                        {row.auditSet
                          .reduce((total, item) => total + item.count, 0)
                          .toLocaleString()}
                      </Table.Summary.Cell>
                    ))}
                  </Table.Summary.Row>
                </Table.Summary>
              ),
            }}
          />
        </div>
      </div>
    </>
  );
};

export default Comp;
