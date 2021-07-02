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

import React, { useState, useMemo } from 'react';
import { Button, Modal, message } from 'antd';
import HighTable from '@/components/HighTable';
import { defaultSize } from '@/configs/pagination';
import { useRequest } from '@/hooks';
import { DataStorageDetailModal, dataStorageHiveColumns } from '@/components/AccessHelper';
import request from '@/utils/request';
import { CommonInterface } from '../common';
import { genStatusTag } from './status';

type Props = CommonInterface;

const getFilterFormContent = defaultValues => [
  {
    type: 'inputsearch',
    name: 'keyWord',
    props: {
      placeholder: '请输入关键词',
    },
  },
  {
    type: 'select',
    name: 'storageType',
    label: '类型',
    initialValue: defaultValues.storageType,
    props: {
      dropdownMatchSelectWidth: false,
      options: [
        {
          label: 'HIVE',
          value: 'HIVE',
        },
      ],
    },
  },
];

const Comp: React.FC<Props> = ({ bid }) => {
  const [options, setOptions] = useState({
    keyWord: '',
    pageSize: defaultSize,
    pageNum: 1,
    storageType: 'HIVE',
  });

  const [changedValues, setChangedValues] = useState({}) as any;

  const [createModal, setCreateModal] = useState<Record<string, unknown>>({
    visible: false,
  });

  const { data, loading, run: getList } = useRequest(
    {
      url: '/storage/list',
      params: {
        ...options,
        bid,
      },
    },
    {
      refreshDeps: [options],
    },
  );

  const { data: datastreamList = [] } = useRequest(
    {
      url: '/datastream/list',
      params: {
        pageNum: 1,
        pageSize: 1000,
        bid,
        dataStorageType: options.storageType,
      },
    },
    {
      ready: !!createModal.visible,
      formatResult: result => result?.list || [],
    },
  );

  const onSave = async values => {
    const isUpdate = createModal.id;
    const submitData = {
      ...values,
      storageType: options.storageType,
      businessIdentifier: bid,
    };
    if (isUpdate) {
      submitData.id = createModal.id;
    }
    await request({
      url: isUpdate ? '/storage/update' : '/storage/save',
      method: 'POST',
      data: submitData,
    });
    await getList();
    message.success('保存成功');
  };

  const onEdit = ({ id }) => {
    setCreateModal({ visible: true, id });
  };

  const onDelete = ({ id }) => {
    Modal.confirm({
      title: '确认删除吗',
      onOk: async () => {
        await request({
          url: `/storage/delete/${id}`,
          method: 'DELETE',
          params: {
            storageType: options.storageType,
          },
        });
        await getList();
        message.success('删除成功');
      },
    });
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

  const pagination = {
    pageSize: options.pageSize,
    current: options.pageNum,
    total: data?.total,
  };

  const columnsMap = {
    HIVE: dataStorageHiveColumns,
  };

  const createContent = useMemo(
    () => [
      {
        type: 'select',
        label: '数据流',
        name: 'dataStreamIdentifier',
        props: {
          notFoundContent: '暂无可用数据流，请先创建新数据流',
          disabled: !!createModal.id,
          options: datastreamList.map(item => ({
            label: item.dataStreamIdentifier,
            value: item.dataStreamIdentifier,
          })),
        },
        rules: [{ required: true }],
      },
    ],
    [createModal.id, datastreamList],
  );

  const datastreamItem = datastreamList.find(
    item => item.dataStreamIdentifier === changedValues.dataStreamIdentifier,
  );

  const columns = [
    {
      title: '数据流',
      dataIndex: 'dataStreamIdentifier',
    },
  ]
    .concat(columnsMap[options.storageType])
    .concat([
      {
        title: '状态',
        dataIndex: 'status',
        render: text => genStatusTag(text),
      },
      {
        title: '操作',
        dataIndex: 'action',
        render: (text, record) => (
          <>
            <Button type="link" onClick={() => onEdit(record)}>
              编辑
            </Button>
            <Button type="link" onClick={() => onDelete(record)}>
              删除
            </Button>
          </>
        ),
      } as any,
    ]);

  return (
    <>
      <HighTable
        filterForm={{
          content: getFilterFormContent(options),
          onFilter,
        }}
        suffix={
          <Button type="primary" onClick={() => setCreateModal({ visible: true })}>
            新建流向配置
          </Button>
        }
        table={{
          columns,
          rowKey: 'id',
          dataSource: data?.list,
          pagination,
          loading,
          onChange,
        }}
      />

      <DataStorageDetailModal
        {...createModal}
        bid={bid}
        content={createContent}
        storageType={options.storageType as any}
        visible={createModal.visible as boolean}
        dataType={datastreamItem?.dataType}
        onValuesChange={(c, v) => setChangedValues(v)}
        onOk={async values => {
          await onSave(values);
          setCreateModal({ visible: false });
        }}
        onCancel={() => setCreateModal({ visible: false })}
      />
    </>
  );
};

export default Comp;
