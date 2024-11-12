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

import React, { useEffect, useMemo, useState } from 'react';
import { Button, message, Modal } from 'antd';
import i18n from '@/i18n';
import { ModalProps } from 'antd/es/modal';
import { defaultSize } from '@/configs/pagination';
import { useRequest } from '@/ui/hooks';
import HighTable from '@/ui/components/HighTable';
import { getFormContent, getTableColumns } from '@/ui/pages/Clusters/config';
export interface Props extends ModalProps {
  ip?: string;
  operationType?: string;
}

const Comp: React.FC<Props> = ({ ...modalProps }) => {
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
        ip: modalProps.ip,
        operationTarget: 'CLUSTER_NODE',
      },
    },
    {
      manual: true,
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
    if (modalProps.open) {
      run();
    }
  }, [modalProps.open, options]);

  return (
    <Modal {...modalProps} title={i18n.t('操作日志')} width={1200}>
      <HighTable
        filterForm={{
          content: getFormContent(),
          onFilter,
        }}
        table={{
          columns: getTableColumns,
          rowKey: 'id',
          dataSource: sourceData?.list || [],
          pagination,
          onChange,
        }}
      />
    </Modal>
  );
};

export default Comp;
