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

import React, { useEffect, useState } from 'react';
import HighTable from '@/ui/components/HighTable';
import { useRequest } from '@/ui/hooks';
import { CommonInterface } from '../common';
import { getFormContent, getTableColumns } from './config';
import { defaultSize } from '@/configs/pagination';

type Props = CommonInterface;

const Comp: React.FC<Props> = ({ inlongGroupId }) => {
  const [options, setOptions] = useState({
    pageSize: defaultSize,
    pageNum: 1,
  });

  const { data: sourceData, run } = useRequest(
    {
      url: '/operationLog/list',
      method: 'POST',
      data: {
        ...options,
        inlongGroupId,
      },
    },
    {
      refreshDeps: [options],
    },
  );

  const pagination = {
    pageSize: options.pageSize,
    current: options.pageNum,
    total: sourceData?.total,
  };
  const onChange = ({ current: pageNum, pageSize }) => {
    setOptions(prev => ({
      ...prev,
      pageNum,
      pageSize,
    }));
  };

  const onFilter = allValues => {
    setOptions(prev => ({
      ...prev,
      ...allValues,
      pageNum: 1,
    }));
  };

  useEffect(() => {
    run();
  }, []);

  return (
    <>
      <HighTable
        filterForm={{
          content: getFormContent(inlongGroupId),
          onFilter,
        }}
        table={{
          columns: getTableColumns,
          dataSource: sourceData?.list,
          pagination: pagination,
          rowKey: 'inlongGroupId',
          onChange,
        }}
      />
    </>
  );
};

export default Comp;
