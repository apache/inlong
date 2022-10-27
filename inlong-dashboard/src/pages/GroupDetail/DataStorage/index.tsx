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

import React, { useState, useMemo, forwardRef, useCallback } from 'react';
import { Button, Modal, message } from 'antd';
import HighTable from '@/components/HighTable';
import { defaultSize } from '@/configs/pagination';
import { useRequest } from '@/hooks';
import i18n from '@/i18n';
import DetailModal from './DetailModal';
import { useDefaultMeta, useLoadMeta } from '@/metas';
import request from '@/utils/request';
import { pickObjectArray } from '@/utils';
import { CommonInterface } from '../common';

type Props = CommonInterface;

const Comp = ({ inlongGroupId, readonly }: Props, ref) => {
  const { defaultValue } = useDefaultMeta('sink');

  const [options, setOptions] = useState({
    keyword: '',
    pageSize: defaultSize,
    pageNum: 1,
    sinkType: defaultValue,
  });

  const [createModal, setCreateModal] = useState<Record<string, unknown>>({
    visible: false,
  });

  const {
    data,
    loading,
    run: getList,
  } = useRequest(
    {
      url: '/sink/list',
      params: {
        ...options,
        inlongGroupId,
      },
    },
    {
      refreshDeps: [options],
    },
  );

  const onEdit = useCallback(({ id }) => {
    setCreateModal({ visible: true, id });
  }, []);

  const onDelete = useCallback(
    ({ id }) => {
      Modal.confirm({
        title: i18n.t('basic.DeleteConfirm'),
        onOk: async () => {
          await request({
            url: `/sink/delete/${id}`,
            method: 'DELETE',
            params: {
              sinkType: options.sinkType,
            },
          });
          await getList();
          message.success(i18n.t('basic.DeleteSuccess'));
        },
      });
    },
    [getList, options.sinkType],
  );

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

  const { Entity } = useLoadMeta('sink', options.sinkType);

  const getFilterFormContent = useCallback(
    defaultValues => [
      {
        type: 'inputsearch',
        name: 'keyword',
      },
      ...pickObjectArray(['sinkType', 'status'], Entity?.FieldList).map(item => ({
        ...item,
        visible: true,
        initialValue: defaultValues[item.name],
      })),
    ],
    [Entity?.FieldList],
  );

  const columns = useMemo(() => {
    if (!Entity) return [];

    return Entity.ColumnList?.concat([
      {
        title: i18n.t('basic.Operating'),
        dataIndex: 'action',
        render: (text, record) =>
          readonly ? (
            '-'
          ) : (
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
  }, [Entity, onDelete, onEdit, readonly]);

  return (
    <>
      <HighTable
        filterForm={{
          content: getFilterFormContent(options),
          onFilter,
        }}
        suffix={
          !readonly && (
            <Button type="primary" onClick={() => setCreateModal({ visible: true })}>
              {i18n.t('pages.GroupDetail.Sink.New')}
            </Button>
          )
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

      <DetailModal
        {...createModal}
        inlongGroupId={inlongGroupId}
        visible={createModal.visible as boolean}
        onOk={async () => {
          await getList();
          setCreateModal({ visible: false });
        }}
        onCancel={() => setCreateModal({ visible: false })}
      />
    </>
  );
};

export default forwardRef(Comp);
