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
import i18n from '@/i18n';
import request from '@/utils/request';
import { CommonInterface } from '../common';
import { genStatusTag } from './status';

type Props = CommonInterface;

const getFilterFormContent = defaultValues => [
  {
    type: 'inputsearch',
    name: 'keyWord',
  },
  {
    type: 'radiobutton',
    name: 'type',
    label: i18n.t('pages.AccessDetail.DataSources.Type'),
    initialValue: defaultValues.type,
    props: {
      buttonStyle: 'solid',
      options: [
        {
          label: i18n.t('pages.AccessDetail.DataSources.File'),
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
    message.success(i18n.t('pages.AccessDetail.DataSources.SaveSuccessfully'));
  };

  const onEdit = ({ id }) => {
    setCreateModal({ visible: true, id });
  };

  const onDelete = ({ id }) => {
    Modal.confirm({
      title: i18n.t('pages.AccessDetail.DataSources.DeletConfirm'),
      onOk: async () => {
        await request({
          url: `/datasource/${options.type}/deleteDetail/${id}`,
          method: 'DELETE',
        });
        await getList();
        message.success(i18n.t('pages.AccessDetail.DataSources.DeleteSuccessfully'));
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
      title: i18n.t('pages.AccessDetail.DataSources.DataStreams'),
      dataIndex: 'dataStreamIdentifier',
      width: 100,
    } as any,
  ]
    .concat(options.type === 'file' ? dataSourcesFileColumns : dataSourcesDbColumns)
    .concat([
      {
        title: i18n.t('basic.Status'),
        dataIndex: 'status',
        render: text => genStatusTag(text),
      },
      {
        title: i18n.t('basic.Operating'),
        dataIndex: 'action',
        render: (text, record) => (
          <>
            <Button type="link" onClick={() => onEdit(record)}>
              {i18n.t('basic.Edit')}
            </Button>
            <Button type="link" onClick={() => onDelete(record)}>
              {i18n.t('basic.Delete')}
            </Button>
          </>
        ),
      } as any,
    ]);

  const createContent = [
    {
      type: 'select',
      label: i18n.t('pages.AccessDetail.DataSources.DataStreams'),
      name: 'dataStreamIdentifier',
      props: {
        notFoundContent: i18n.t('pages.AccessDetail.DataSources.NoDataStreams'),
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
            {i18n.t('pages.AccessDetail.DataSources.Create')}
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
