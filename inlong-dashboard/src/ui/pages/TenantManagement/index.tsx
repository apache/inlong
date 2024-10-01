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

import React, { useCallback, useEffect, useState } from 'react';
import { Button, Card, message, Modal } from 'antd';
import { PageContainer, Container } from '@/ui/components/PageContainer';
import HighTable from '@/ui/components/HighTable';
import { useRequest } from '@/ui/hooks';
import { useTranslation } from 'react-i18next';
import { defaultSize } from '@/configs/pagination';
import DetailModal from './DetailModal';
import { getFilterFormContent, getColumns } from './config';
import i18n from 'i18next';
import request from '@/core/utils/request';

const Comp: React.FC = () => {
  const { t } = useTranslation();

  const [tenantList, setTenantList] = useState([]);
  const [options, setOptions] = useState({
    keyword: '',
    pageSize: defaultSize,
    pageNum: 1,
    tenantList: tenantList,
  });

  const [createModal, setCreateModal] = useState<Record<string, unknown>>({
    open: false,
  });

  const { run: getTenantData } = useRequest(
    {
      url: '/tenant/list',
      method: 'POST',
      data: {
        pageNum: 1,
        pageSize: 9999,
        listByLoginUser: true,
      },
    },
    {
      manual: true,
      onSuccess: result => {
        const list = result.list.map(item => item.name);
        setOptions(prev => ({
          ...prev,
          tenantList: list,
        }));
        setTenantList(list);
        getList();
      },
    },
  );

  const {
    data,
    loading,
    run: getList,
  } = useRequest(
    {
      url: '/role/tenant/list',
      method: 'POST',
      data: options,
    },
    {
      refreshDeps: [options],
      manual: tenantList.length > 0 ? false : true,
    },
  );

  const onEdit = ({ id }) => {
    setCreateModal({
      open: true,
      id,
    });
  };

  const onChange = ({ current: pageNum, pageSize }) => {
    setOptions(prev => ({
      ...prev,
      pageNum,
      pageSize,
    }));
  };

  const onFilter = keyword => {
    setOptions(prev => ({
      ...prev,
      ...keyword,
      pageNum: 1,
    }));
  };

  useEffect(() => {
    getTenantData();
  }, []);

  const pagination = {
    pageSize: options.pageSize,
    current: options.pageNum,
    total: data?.total,
  };
  const onDelete = useCallback(
    ({ id }) => {
      Modal.confirm({
        title: i18n.t('basic.DeleteConfirm'),
        onOk: async () => {
          await request({
            url: `/role/tenant/delete/${id}`,
            method: 'DELETE',
          });
          await getList();
          message.success(i18n.t('basic.DeleteSuccess'));
        },
      });
    },
    [getList],
  );
  return (
    <PageContainer useDefaultBreadcrumb={false} useDefaultContainer={false}>
      <Container>
        <Card>
          <HighTable
            suffix={
              <Button type="primary" onClick={() => setCreateModal({ open: true })}>
                {t('pages.TenantRole.New')}
              </Button>
            }
            filterForm={{
              content: getFilterFormContent(),
              onFilter,
            }}
            table={{
              columns: getColumns({ onEdit, onDelete }),
              rowKey: 'id',
              dataSource: data?.list,
              pagination,
              loading,
              onChange,
            }}
          />
        </Card>
      </Container>

      <DetailModal
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
