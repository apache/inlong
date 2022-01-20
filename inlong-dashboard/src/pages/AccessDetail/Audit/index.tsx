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

import React, { useState } from 'react';
import FormGenerator, { useForm } from '@/components/FormGenerator';
import HighTable from '@/components/HighTable';
import { useRequest } from '@/hooks';
import { useTranslation } from 'react-i18next';
import { timestampFormat } from '@/utils';
import Charts from '@/components/Charts';
import { CommonInterface } from '../common';
import { getFormContent, toChartData, toTableData, getTableColumns, auditList } from './config';

type Props = CommonInterface;

const Comp: React.FC<Props> = ({ inlongGroupId }) => {
  const { t } = useTranslation();

  const [form] = useForm();

  const [query, setQuery] = useState({
    inlongStreamId: '',
    auditIds: auditList.slice(0, 2).map(item => item.value),
    dt: +new Date(),
  });

  // const test = [
  //   {
  //     auditId: 1,
  //     auditSet: [
  //       {
  //         logTs: '2022-01-01',
  //         count: 123,
  //       },
  //       {
  //         logTs: '2022-01-02',
  //         count: 83,
  //       },
  //       {
  //         logTs: '2022-01-03',
  //         count: 126,
  //       },
  //     ],
  //   },
  //   {
  //     auditId: 2,
  //     auditSet: [
  //       {
  //         logTs: '2022-01-01',
  //         count: 153,
  //       },
  //       {
  //         logTs: '2022-01-02',
  //         count: 101,
  //       },
  //       {
  //         logTs: '2022-01-03',
  //         count: 121,
  //       },
  //     ],
  //   },
  // ];
  const { data = [], run } = useRequest(
    {
      url: '/audit/list',
      params: {
        ...query,
        auditIds: query.auditIds?.join(','),
        dt: timestampFormat(query.dt, 'yyyy-MM-dd'),
        inlongGroupId,
      },
    },
    {
      ready: Boolean(query.inlongStreamId && query.auditIds?.length),
    },
  );

  const onSearch = async () => {
    await form.validateFields();
    run();
  };

  return (
    <>
      <div style={{ marginBottom: 40 }}>
        <FormGenerator
          form={form}
          layout="inline"
          content={getFormContent(inlongGroupId, query, onSearch)}
          style={{ marginBottom: 30 }}
          onFilter={allValues =>
            setQuery({
              ...allValues,
              dt: +allValues.dt.$d,
            })
          }
        />
        <Charts height={400} isEmpty={!data.length} option={toChartData(data)} />
      </div>

      <HighTable
        table={{
          columns: getTableColumns(data),
          dataSource: toTableData(data),
          rowKey: 'logTs',
        }}
      />
    </>
  );
};

export default Comp;
