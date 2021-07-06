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
import { Button, Modal, message } from 'antd';
import HighTable from '@/components/HighTable';
import { defaultSize } from '@/configs/pagination';
import { useRequest } from '@/hooks';
import {
  dataSourcesDbColumns,
  dataSourcesFileColumns,
  DataSourcesCreateModal,
} from '@/components/AccessHelper';
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
    type: 'radiobutton',
    name: 'type',
    label: '类型',
    initialValue: defaultValues.type,
    props: {
      buttonStyle: 'solid',
      options: [
        {
          label: '文件',
          value: 'file',
        },
        // {
        //   label: 'DB',
        //   value: 'db',
        // },
      ],
    },
  },
];

const Comp: React.FC<Props> = ({ bid }) => {
  const [options, setOptions] = useState({
    // keyWord: '',
    pageSize: defaultSize,
    pageNum: 1,
    type: 'file',
  });

  const [createModal, setCreateModal] = useState<Record<string, unknown>>({
    visible: false,
  });

  const { data, loading, run: getList } = useRequest(
    {
      url: `/datasource/${options.type}/listDetail/`,
      params: {
        ...options,
        type: undefined,
        bid,
      },
    },
    {
      refreshDeps: [options],
    },
  );

  const onSave = async values => {
    const isUpdate = createModal.id;
    const submitData = {
      ...values,
      businessIdentifier: bid,
    };
    if (isUpdate) {
      submitData.id = createModal.id;
    }
    await request({
      url: `/datasource/${options.type}/${isUpdate ? 'updateDetail' : 'saveDetail'}`,
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
          url: `/datasource/${options.type}/deleteDetail/${id}`,
          method: 'DELETE',
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

  const columns = [
    {
      title: '数据流',
      dataIndex: 'dataStreamIdentifier',
      width: 100,
    } as any,
  ]
    .concat(options.type === 'file' ? dataSourcesFileColumns : dataSourcesDbColumns)
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

  const createContent = [
    {
      type: 'select',
      label: '数据流',
      name: 'dataStreamIdentifier',
      props: {
        notFoundContent: '暂无可用数据流，请先创建新数据流',
        disabled: !!createModal.id,
        options: {
          requestService: {
            url: '/datastream/list',
            params: {
              pageNum: 1,
              pageSize: 1000,
              bid,
              dataSourceType: options.type,
            },
          },
          requestParams: {
            ready: !!(createModal.visible && !createModal.id),
            formatResult: result =>
              result?.list.map(item => ({
                label: item.dataStreamIdentifier,
                value: item.dataStreamIdentifier,
              })) || [],
          },
        },
      },
      rules: [{ required: true }],
    },
  ];

  return (
    <>
      <HighTable
        filterForm={{
          content: getFilterFormContent(options),
          onFilter,
        }}
        suffix={
          <Button type="primary" onClick={() => setCreateModal({ visible: true })}>
            新建数据源
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

      <DataSourcesCreateModal
        {...createModal}
        type={options.type.toUpperCase() as any}
        content={createContent}
        visible={createModal.visible as boolean}
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
