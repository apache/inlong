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
import { Button, Modal, message, Dropdown, Space } from 'antd';
import i18n from '@/i18n';
import { parse } from 'qs';
import HighTable from '@/ui/components/HighTable';
import { PageContainer } from '@/ui/components/PageContainer';
import { defaultSize } from '@/configs/pagination';
import { useRequest, useLocation } from '@/ui/hooks';
import NodeEditModal from './NodeEditModal';
import request from '@/core/utils/request';
import { timestampFormat } from '@/core/utils';
import { genStatusTag } from './status';
import HeartBeatModal from '@/ui/pages/Clusters/HeartBeatModal';
import LogModal from '@/ui/pages/Clusters/LogModal';
import { DownOutlined } from '@ant-design/icons';
import { MenuProps } from 'antd/es/menu';

const getFilterFormContent = defaultValues => [
  {
    type: 'inputsearch',
    name: 'keyword',
  },
];

const Comp: React.FC = () => {
  const location = useLocation();
  const { type, clusterId } = useMemo<Record<string, string>>(
    () => (parse(location.search.slice(1)) as Record<string, string>) || {},
    [location.search],
  );

  const [options, setOptions] = useState({
    keyword: '',
    pageSize: defaultSize,
    pageNum: 1,
    type,
    parentId: +clusterId,
  });

  const [nodeEditModal, setNodeEditModal] = useState<Record<string, unknown>>({
    open: false,
  });
  const [logModal, setLogModal] = useState<Record<string, unknown>>({
    open: false,
  });
  const [heartModal, setHeartModal] = useState<Record<string, unknown>>({
    open: false,
  });

  const {
    data,
    loading,
    run: getList,
  } = useRequest(
    {
      url: '/cluster/node/list',
      method: 'POST',
      data: {
        ...options,
      },
    },
    {
      refreshDeps: [options],
    },
  );

  const onEdit = ({ id }) => {
    setNodeEditModal({ open: true, id });
  };
  const onUnload = useCallback(
    ({ id }) => {
      Modal.confirm({
        title: i18n.t('pages.Cluster.Node.UnloadTitle'),
        onOk: async () => {
          await request({
            url: `/cluster/node/unload/${id}`,
            method: 'DELETE',
          });
          await getList();
          message.success(i18n.t('basic.OperatingSuccess'));
        },
      });
    },
    [getList],
  );
  const onRestart = useCallback(
    record => {
      Modal.confirm({
        title: i18n.t('pages.Cluster.Node.RestartTitle'),
        onOk: async () => {
          record.agentRestartTime = record?.agentRestartTime + 1;
          delete record.isInstall;
          await request({
            url: `/cluster/node/update`,
            method: 'POST',
            data: record,
          });
          await getList();
          message.success(i18n.t('basic.OperatingSuccess'));
        },
      });
    },
    [getList],
  );
  const onInstall = useCallback(
    record => {
      Modal.confirm({
        title: i18n.t('pages.Cluster.Node.InstallTitle'),
        onOk: async () => {
          await request({
            url: `/cluster/node/update`,
            method: 'POST',
            data: {
              ...record,
              isInstall: true,
            },
          });
          await getList();
          message.success(i18n.t('basic.OperatingSuccess'));
        },
      });
    },
    [getList],
  );

  const onLog = ({ id }) => {
    setLogModal({ open: true, id });
  };
  const openHeartModal = ({ type, ip }) => {
    setHeartModal({ open: true, type: type, ip: ip });
  };
  const onDelete = useCallback(
    ({ id }) => {
      Modal.confirm({
        title: i18n.t('basic.DeleteConfirm'),
        onOk: async () => {
          await request({
            url: `/cluster/node/delete/${id}`,
            method: 'DELETE',
          });
          await getList();
          message.success(i18n.t('basic.DeleteSuccess'));
        },
      });
    },
    [getList],
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
    pageSize: +options.pageSize,
    current: +options.pageNum,
    total: data?.total,
  };
  const [operationType, setOperationType] = useState('');

  const { data: nodeData, run: getNodeData } = useRequest(
    id => ({
      url: `/cluster/node/get/${id}`,
    }),
    {
      manual: true,
      onSuccess: result => {
        switch (operationType) {
          case 'onRestart':
            onRestart(result);
            break;
          case 'onUnload':
            onUnload(result);
            break;
          case 'onInstall':
            onInstall(result);
            break;
          default:
            break;
        }
      },
    },
  );
  const items: MenuProps['items'] = [
    {
      label: <Button type="link">{i18n.t('pages.Cluster.Node.Install')}</Button>,
      key: '0',
    },
    {
      label: <Button type="link">{i18n.t('pages.Nodes.Restart')}</Button>,
      key: '1',
    },
    {
      label: <Button type="link">{i18n.t('pages.Cluster.Node.Unload')}</Button>,
      key: '2',
    },
    {
      label: <Button type="link">{i18n.t('pages.Cluster.Node.InstallLog')}</Button>,
      key: '3',
    },
    {
      label: <Button type="link">{i18n.t('pages.Clusters.Node.Agent.HeartbeatDetection')}</Button>,
      key: '4',
    },
  ];
  const handleMenuClick = (key, record) => {
    switch (key) {
      case '0':
        getNodeData(record.id).then(() => setOperationType('onInstall'));
        break;
      case '1':
        getNodeData(record.id).then(() => setOperationType('onRestart'));
        break;
      case '2':
        getNodeData(record.id).then(() => setOperationType('onUnload'));
        break;
      case '3':
        onLog(record);
        break;
      case '4':
        openHeartModal(record);
        break;
      default:
        break;
    }
  };
  const columns = useMemo(() => {
    return [
      {
        title: 'IP',
        dataIndex: 'ip',
      },
      {
        title: i18n.t('pages.Clusters.Node.Port'),
        dataIndex: 'port',
      },
      {
        title: i18n.t('pages.Clusters.Node.Online'),
        dataIndex: 'enabledOnline',
        render: text => (text !== undefined ? text.toString() : ''),
      },
      {
        title: i18n.t('pages.Clusters.Node.ProtocolType'),
        dataIndex: 'protocolType',
      },
      {
        title: i18n.t('pages.Clusters.Node.Status'),
        dataIndex: 'status',
        render: text => genStatusTag(text),
      },
      {
        title: i18n.t('pages.Clusters.Node.Creator'),
        dataIndex: 'creator',
        render: (text, record: any) => (
          <>
            <div>{text}</div>
            <div>{record.createTime && timestampFormat(record.createTime)}</div>
          </>
        ),
      },
      {
        title: i18n.t('pages.Clusters.Node.LastModifier'),
        dataIndex: 'modifier',
        render: (text, record: any) => (
          <>
            <div>{text}</div>
            <div>{record.modifyTime && timestampFormat(record.modifyTime)}</div>
          </>
        ),
      },
      {
        title: i18n.t('basic.Operating'),
        dataIndex: 'action',
        key: 'operation',
        width: 200,
        render: (text, record) => (
          <>
            <Button type="link" onClick={() => onEdit(record)}>
              {i18n.t('basic.Edit')}
            </Button>
            <Button type="link" onClick={() => onDelete(record)}>
              {i18n.t('basic.Delete')}
            </Button>
            {type === 'AGENT' && (
              <Dropdown menu={{ items, onClick: ({ key }) => handleMenuClick(key, record) }}>
                <a onClick={e => e.preventDefault()}>
                  <Space>
                    {i18n.t('pages.Cluster.Node.More')}
                    <DownOutlined />
                  </Space>
                </a>
              </Dropdown>
            )}
          </>
        ),
      },
    ];
  }, [onDelete]);

  return (
    <PageContainer
      breadcrumb={[
        {
          name: `${type === 'DATAPROXY' ? 'DataProxy' : 'Agent'} ${i18n.t(
            'pages.Clusters.Node.Name',
          )}`,
        },
      ]}
    >
      <HighTable
        filterForm={{
          content: getFilterFormContent(options),
          onFilter,
        }}
        suffix={
          <Button type="primary" onClick={() => setNodeEditModal({ open: true })}>
            {i18n.t('pages.Clusters.Node.Create')}
          </Button>
        }
        table={{
          columns:
            type === 'AGENT'
              ? columns.filter(
                  item =>
                    item.dataIndex !== 'enabledOnline' &&
                    item.dataIndex !== 'port' &&
                    item.dataIndex !== 'protocolType',
                )
              : columns,
          rowKey: 'id',
          dataSource: data?.list,
          pagination,
          loading,
          onChange,
        }}
      />

      <NodeEditModal
        type={type}
        clusterId={+clusterId}
        {...nodeEditModal}
        open={nodeEditModal.open as boolean}
        onOk={async () => {
          await getList();
          setNodeEditModal({ open: false });
        }}
        onCancel={() => setNodeEditModal({ open: false })}
      />
      <LogModal
        {...logModal}
        open={logModal.open as boolean}
        onOk={async () => {
          await getList();
          setLogModal({ open: false });
        }}
        onCancel={() => setLogModal({ open: false })}
      />
      <HeartBeatModal
        {...heartModal}
        open={heartModal.open as boolean}
        onOk={async () => {
          await getList();
          setHeartModal({ open: false });
        }}
        onCancel={() => setHeartModal({ open: false })}
      />
    </PageContainer>
  );
};

export default Comp;
