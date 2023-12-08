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

import React, { useMemo, useState } from 'react';
import { useForm } from '@/ui/components/FormGenerator';
import HighTable from '@/ui/components/HighTable';
import { useRequest } from '@/ui/hooks';
import { timestampFormat } from '@/core/utils';
import { getFormContent, toTableData, getTableColumns, timeStaticsDimList } from './config';

const Comp: React.FC = () => {
  const [form] = useForm();

  const [query, setQuery] = useState({
    startDate: +new Date(),
    endDate: +new Date(),
    auditIds: ['3', '4'],
    timeStaticsDim: timeStaticsDimList[0].value,
  });

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
      refreshDeps: [query],
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
    await form.validateFields();
    run();
  };

  const onFilter = keyword => {
    setQuery({
      ...query,
      ...keyword,
      auditIds:
        keyword.benchmark !== undefined && keyword.compared !== undefined
          ? [keyword.benchmark, keyword.compared]
          : ['3', '4'],
      startDate: +keyword.startDate.$d,
      endDate: +keyword.startDate.$d,
    });
  };

  return (
    <>
      <HighTable
        filterForm={{
          content: getFormContent(query, onSearch),
          onFilter,
        }}
        table={{
          columns: getTableColumns(sourceData),
          dataSource: toTableData(sourceData, sourceDataMap),
          rowKey: 'logTs',
        }}
      />
    </>
  );
};

export default Comp;
