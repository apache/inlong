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

import React, { useMemo } from 'react';
import { Modal } from 'antd';
import { ModalProps } from 'antd/es/modal';
import { useRequest, useUpdateEffect } from '@/ui/hooks';
import i18n from '@/i18n';
import HighTable from '@/ui/components/HighTable';
import { timestampFormat } from '@/core/utils';

export interface Props extends ModalProps {
  type?: string;
  ip?: string;
}

const Comp: React.FC<Props> = ({ type, ip, ...modalProps }) => {
  const { data: heartList, run: getHeartList } = useRequest(
    {
      url: '/heartbeat/component/list',
      method: 'POST',
      data: {
        component: type,
        inlongGroupId: '',
        inlongStreamId: '',
        instance: ip,
      },
    },
    {
      manual: true,
      onSuccess: data => {
        console.log(data);
      },
    },
  );

  const columns = useMemo(() => {
    return [
      {
        title: i18n.t('pages.Clusters.Node.Agent.HeartbeatInfo.Component'),
        dataIndex: 'component',
      },
      {
        title: i18n.t('pages.Clusters.Node.Agent.HeartbeatInfo.Instance'),
        dataIndex: 'instance',
      },
      {
        title: i18n.t('pages.Clusters.Node.Agent.HeartbeatInfo.ModifyTime'),
        dataIndex: 'modifyTime',
        render: (text, record: any) => (
          <>
            <div>{record.modifyTime && timestampFormat(record.modifyTime)}</div>
          </>
        ),
      },
      {
        title: i18n.t('pages.Clusters.Node.Agent.HeartbeatInfo.ReportTime'),
        dataIndex: 'reportTime',
        render: (text, record: any) => (
          <>
            <div>{record.modifyTime && timestampFormat(record.modifyTime)}</div>
          </>
        ),
      },
    ];
  }, []);
  const pagination = {
    pageSize: 5,
    current: 1,
    total: heartList?.list?.length,
  };
  useUpdateEffect(() => {
    if (modalProps.open) {
      getHeartList();
    }
  }, [modalProps.open]);

  return (
    <Modal
      {...modalProps}
      title={i18n.t('pages.Clusters.Node.Agent.HeartbeatInfo')}
      width={1200}
      footer={null}
    >
      <HighTable
        table={{
          columns: columns,
          rowKey: 'id',
          dataSource: heartList?.list,
          pagination,
        }}
      />
    </Modal>
  );
};

export default Comp;
