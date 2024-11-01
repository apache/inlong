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

import React, { useCallback, useMemo, useState } from 'react';
import { Button, message, Modal } from 'antd';
import i18n from '@/i18n';
import HighTable from '@/ui/components/HighTable';
import { PageContainer } from '@/ui/components/PageContainer';
import { defaultSize } from '@/configs/pagination';
import { useRequest } from '@/ui/hooks';
import CreateModal from './CreateModal';
import { timestampFormat } from '@/core/utils';
import request from '@/core/utils/request';

const Comp: React.FC = () => {
  const [options, setOptions] = useState({
    inCharges: null,
    name: null,
    tenantList: null,
    orderField: null,
    orderType: null,
    pageNum: 1,
    pageSize: defaultSize,
    visibleRange: null,
  });

  const [createModal, setCreateModal] = useState<Record<string, unknown>>({
    open: false,
  });

  const {
    data,
    loading,
    run: getList,
  } = useRequest(
    {
      url: '/template/list',
      method: 'POST',
      data: {
        ...options,
      },
    },
    {
      refreshDeps: [options],
      onSuccess: result => {},
    },
  );

  const onEdit = ({ id, name }) => {
    setCreateModal({ open: true, id, templateName: name });
  };

  const onChange = ({ current: pageNum, pageSize }) => {
    setOptions(prev => ({
      ...prev,
      pageNum,
      pageSize,
    }));
  };

  const onFilter = allValues => {
    for (const key in allValues) {
      if (allValues[key] === '') {
        allValues[key] = null;
      }
    }
    setOptions(prev => ({
      ...prev,
      ...allValues,
      pageNum: 1,
    }));
  };

  const pagination = {
    pageSize: +options.pageSize,
    current: +options.pageNum,
    total: data?.total,
  };

  const getFilterFormContent = useCallback(
    defaultValues => [
      {
        type: 'inputsearch',
        name: 'name',
      },
    ],
    [],
  );
  const onDelete = useCallback(
    record => {
      Modal.confirm({
        title: i18n.t('basic.DeleteConfirm'),
        onOk: async () => {
          await request({
            url: `/template/delete`,
            method: 'DELETE',
            params: {
              templateName: record.name,
            },
          });
          await getList();
          message.success(i18n.t('basic.DeleteSuccess'));
        },
      });
    },
    [getList],
  );
  const entityColumns = useMemo(() => {
    return [
      {
        title: i18n.t('pages.GroupDataTemplate.Name'),
        dataIndex: 'name',
        key: 'name',
        width: 200,
      },
      {
        title: i18n.t('pages.GroupDataTemplate.InCharges'),
        dataIndex: 'inCharges',
        key: 'inCharges',
        width: 300,
      },
      {
        title: i18n.t('pages.GroupDataTemplate.TenantList'),
        dataIndex: 'tenantList',
        key: 'tenantList',
        width: 200,
        render: (text, record: any) => (
          <>
            <div>{record.tenantList?.join(',')}</div>
          </>
        ),
      },
      {
        title: i18n.t('pages.GroupDataTemplate.Creator'),
        dataIndex: 'creator',
        key: 'creator',
        width: 200,
        render: (text, record: any) => (
          <>
            <div>{text}</div>
            <div>{record.createTime && timestampFormat(record.createTime)}</div>
          </>
        ),
      },
      {
        title: i18n.t('pages.GroupDataTemplate.Modifier'),
        dataIndex: 'modifier',
        key: 'modifier',
        width: 200,
        render: (text, record: any) => (
          <>
            <div>{text}</div>
            <div>{record.modifyTime && timestampFormat(record.modifyTime)}</div>
          </>
        ),
      },
    ];
  }, []);
  const columns = useMemo(() => {
    return entityColumns?.concat([
      {
        title: i18n.t('basic.Operating'),
        dataIndex: 'action',
        width: 150,
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
  }, [entityColumns]);

  return (
    <PageContainer useDefaultBreadcrumb={false}>
      <HighTable
        filterForm={{
          content: getFilterFormContent(options),
          onFilter,
        }}
        suffix={
          <Button type="primary" onClick={() => setCreateModal({ open: true })}>
            {i18n.t('pages.GroupDataTemplate.Create')}
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

      <CreateModal
        {...createModal}
        open={createModal.open as boolean}
        onOk={async () => {
          await getList();
          setCreateModal({ open: false });
        }}
        onCancel={() => setCreateModal({ open: false })}
      />
    </PageContainer>
  );
};

export default Comp;
