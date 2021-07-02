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
import { Button, Modal } from 'antd';
import { ModalProps } from 'antd/es/modal';
import HighTable from '@/components/HighTable';
import { defaultSize } from '@/configs/pagination';
import { useRequest, useUpdateEffect } from '@/hooks';

export interface MyAccessModalProps extends Omit<ModalProps, 'onOk'> {
  id?: string;
  onOk?: (value: string, record: Record<string, unknown>) => void;
}

const getFilterFormContent = () => [
  {
    type: 'inputsearch',
    name: 'keyWord',
    props: {
      placeholder: '请输入关键词',
    },
  },
];

const Comp: React.FC<MyAccessModalProps> = ({ id, ...modalProps }) => {
  const [options, setOptions] = useState({
    keyWord: '',
    pageSize: defaultSize,
    pageIndex: 1,
  });

  const { run: getData, data, loading } = useRequest(
    {
      url: '/business/list',
      params: options,
    },
    {
      manual: true,
    },
  );

  useUpdateEffect(() => {
    if (modalProps.visible) {
      getData(id);
    }
  }, [modalProps.visible, id]);

  const onChange = ({ current: pageIndex, pageSize }) => {
    setOptions(prev => ({
      ...prev,
      pageIndex,
      pageSize,
    }));
  };

  const onFilter = allValues => {
    setOptions(prev => ({
      ...prev,
      ...allValues,
      pageIndex: 1,
    }));
  };

  const onOk = record => {
    const { businessIdentifier } = record;
    modalProps.onOk && modalProps.onOk(businessIdentifier, record);
  };

  const columns = [
    {
      title: '业务ID',
      dataIndex: 'businessIdentifier',
    },
    {
      title: '业务名称',
      dataIndex: 'cnName',
    },
    {
      title: '责任人',
      dataIndex: 'inCharges',
    },
    {
      title: '创建时间',
      dataIndex: 'createTime',
    },
    {
      title: '操作',
      dataIndex: 'action',
      render: (text, record) => (
        <Button type="link" onClick={() => onOk(record)}>
          选择
        </Button>
      ),
    },
  ];

  const pagination = {
    pageSize: options.pageSize,
    current: options.pageIndex,
    total: data?.totalSize,
  };

  return (
    <Modal {...modalProps} title="我的接入业务" width={1024} footer={null} onOk={onOk}>
      <HighTable
        filterForm={{
          content: getFilterFormContent(),
          onFilter,
        }}
        table={{
          columns,
          rowKey: 'id',
          size: 'small',
          dataSource: data?.list,
          pagination,
          loading,
          onChange,
        }}
      />
    </Modal>
  );
};

export default Comp;
