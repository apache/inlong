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
import HighTable, { useForm } from '@/ui/components/HighTable';
import { useRequest } from '@/ui/hooks';
import { timestampFormat } from '@/core/utils';
import { getFormContent, toTableData, getTableColumns, sumSubValue } from './config';
import i18n from '@/i18n';
import { AuditProps } from '@/ui/pages/ModuleAudit';
import { Table } from 'antd';
import dayjs from 'dayjs';

export const idModule = 'id';
const Comp: React.FC<AuditProps> = ({ auditData }) => {
  const [form] = useForm();

  const [query, setQuery] = useState({
    startDate: dayjs().startOf('hour').valueOf(),
    endDate: dayjs().startOf('hour').valueOf(),
    auditIds: ['3', '4'],
    inlongGroupId: '',
    inlongStreamId: '',
  });
  const [inlongStreamID, setInlongStreamID] = useState('');
  const [inlongGroupId, setInlongGroupId] = useState('');
  const { data: sourceData = [], run } = useRequest(
    {
      url: '/audit/listAll',
      method: 'POST',
      data: {
        ...query,
        startDate: timestampFormat(query.startDate, 'yyyy-MM-dd HH:mm:ss'),
        endDate: timestampFormat(query.endDate, 'yyyy-MM-dd HH:mm:ss'),
      },
    },
    {
      refreshDeps: [query],
      formatResult: result => {
        const base = result.find(item2 => item2.auditId === query.auditIds[0].toString());
        const compared = result.find(item2 => item2.auditId === query.auditIds[1].toString());
        return [base, compared];
      },
    },
  );

  const sourceDataMap = useMemo(() => {
    if (!sourceData) {
      return {};
    }
    let baseData =
      sourceData[0]?.auditSet?.length > sourceData[1]?.auditSet?.length
        ? sourceData[0]
        : sourceData[1];
    const output = baseData?.auditSet?.reduce((acc, cur) => {
      acc[cur.ip] = {
        ip: cur.ip,
        base:
          sourceData[0].auditId === baseData.auditId
            ? cur.count
            : sourceData[0].auditSet.find(item => (item.ip = cur.ip))
            ? sourceData[0].auditSet.find(item => (item.ip = cur.ip)).count
            : 0,
        compared:
          sourceData[1].auditId === baseData.auditId
            ? cur.count
            : sourceData[1].auditSet.find(item => (item.ip = cur.ip))
            ? sourceData[1].auditSet.find(item => (item.ip = cur.ip)).count
            : 0,
      };
      return acc;
    }, {});
    if (output === undefined || output === null) {
      return {};
    }
    Object.keys(output).forEach(key => {
      output[key] = {
        ...output[key],
        subValue: output[key].compared - output[key].base,
      };
    });
    return output;
  }, [sourceData]);

  const onSearch = async () => {
    await form.validateFields();
    run();
  };

  const onFilter = keyword => {
    setQuery({
      ...query,
      ...keyword,
      auditIds: [
        keyword.benchmark !== undefined ? keyword.benchmark : query.auditIds[0],
        keyword.compared !== undefined ? keyword.compared : query.auditIds[1],
      ],
      inlongGroupId: keyword.inlongGroupId,
      inlongStreamId: keyword.inlongStreamId,
      startDate: +keyword.startDate.$d,
      endDate: keyword.endDate === undefined ? +keyword.startDate.$d : +keyword.endDate.$d,
    });
  };
  const numToName = useCallback(
    num => {
      let obj = {};
      obj = {
        base: sourceData[0].auditName,
        compared: sourceData[1].auditName,
        ip: i18n.t('pages.ModuleAudit.config.Ip'),
        subValue: i18n.t('pages.ModuleAudit.config.SubValue'),
      };
      return obj[num];
    },
    [sourceData],
  );

  const csvData = useMemo(() => {
    const result = toTableData(sourceData, sourceDataMap).map(item => {
      let obj = {};
      Object.keys(item)
        .filter(key => key !== 'logTs')
        .forEach(key => {
          obj = { ...obj, [numToName(key)]: item[key] };
        });
      return obj;
    });
    return result;
  }, [sourceData, sourceDataMap]);
  const [fileName, setFileName] = useState('metrics.csv');
  useEffect(() => {
    setFileName(`id_${inlongGroupId}_${inlongStreamID}.csv`);
  }, [inlongGroupId, inlongStreamID]);
  return (
    <>
      <HighTable
        filterForm={{
          style: { gap: '10px' },
          content: getFormContent(
            query,
            onSearch,
            auditData,
            sourceData,
            csvData,
            setInlongGroupId,
            setInlongStreamID,
            fileName,
          ),
          onFilter,
        }}
        table={{
          columns: getTableColumns(sourceData),
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
                <Table.Summary.Cell key={sourceData.length} index={sourceData.length + 1}>
                  {sumSubValue(sourceDataMap).toLocaleString()}
                </Table.Summary.Cell>
              </Table.Summary.Row>
            </Table.Summary>
          ),
        }}
      />
    </>
  );
};

export default Comp;
