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
import { Button, message, Modal } from 'antd';
import HighTable from '@/ui/components/HighTable';
import { useRequest } from '@/ui/hooks';
import { defaultSize } from '@/configs/pagination';
import { useColumns, getFormContent } from './config';
import CreateModal from './CreateModal';
import i18n from '@/i18n';
import { useLocation } from 'react-router-dom';
import request from '@/core/utils/request';

interface AgentPackageProps {
  AgentPackageType: String;
}

const Comp: React.FC<AgentPackageProps> = ({ AgentPackageType }) => {
  const location = useLocation();
  const type = location.state || AgentPackageType;

  const [query, setQuery] = useState({
    type: type,
    keyword: '',
    pageNum: 1,
    pageSize: defaultSize,
  });

  useEffect(() => {
    setQuery(prev => ({
      ...prev,
      type: type,
    }));
  }, [type]);

  const [createModal, setCreateModal] = useState<Record<string, unknown>>({
    open: false,
  });

  const { data: sourceData = [], run: getList } = useRequest(
    {
      url: '/package/list',
      method: 'POST',
      data: query,
    },
    {
      refreshDeps: [query],
    },
  );

  const pagination = {
    pageSize: query.pageSize,
    current: query.pageNum,
    total: sourceData?.total,
  };

  const onChange = ({ current: pageNum, pageSize }) => {
    setQuery(prev => ({
      ...prev,
      pageNum,
      pageSize,
    }));
  };

  const onFilter = keyword => {
    setQuery({
      ...query,
      ...keyword,
    });
  };

  const openModal = ({ id, type }) => {
    setCreateModal({ open: true, id, type });
  };

  const onDelete = async ({ id }) => {
    Modal.confirm({
      title: i18n.t('basic.DeleteConfirm'),
      onOk: async () => {
        await request({
          url: `/package/delete/${id}`,
          method: 'DELETE',
        });
        await getList();
        message.success(i18n.t('basic.DeleteSuccess'));
      },
    });
  };

  const columns = useColumns({ onDelete, openModal });

  return (
    <>
      <HighTable
        filterForm={{
          content: getFormContent(query),
          onFilter,
        }}
        suffix={
          <Button type="primary" onClick={() => setCreateModal({ open: true, type })}>
            {i18n.t('pages.PackageAgent.Create')}
          </Button>
        }
        table={{
          columns: columns,
          dataSource: sourceData?.list,
          rowKey: 'id',
          pagination,
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
    </>
  );
};

export default Comp;
