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

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
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
import { genStatusTag, statusList } from './status';
import HeartBeatModal from '@/ui/pages/Clusters/HeartBeatModal';
import LogModal from '@/ui/pages/Clusters/LogModal';
import { DownOutlined } from '@ant-design/icons';
import { MenuProps } from 'antd/es/menu';
import { useForm } from 'antd/es/form/Form';
import { getModuleList, versionMap } from '@/ui/pages/Clusters/config';
import AgentBatchUpdateModal from '@/ui/pages/Clusters/AgentBatchUpdateModal';
import OperationLogModal from '@/ui/pages/Clusters/OperationLogModal';

const getFilterFormContent = defaultValues => [
  {
    type: 'inputsearch',
    name: 'keyword',
  },
  {
    type: 'select',
    name: 'status',
    props: {
      options: statusList,
    },
  },
];

const Comp: React.FC = () => {
  const location = useLocation();
  const { type, clusterId } = useMemo<Record<string, string>>(
    () => (parse(location.search.slice(1)) as Record<string, string>) || {},
    [location.search],
  );
  const [form] = useForm();

  const [options, setOptions] = useState({
    keyword: '',
    pageSize: defaultSize,
    pageNum: 1,
    type,
    parentId: +clusterId,
    status: '',
  });

  const [nodeEditModal, setNodeEditModal] = useState<Record<string, unknown>>({
    open: false,
    agentInstallerList: [],
  });
  const [logModal, setLogModal] = useState<Record<string, unknown>>({
    open: false,
  });
  const [heartModal, setHeartModal] = useState<Record<string, unknown>>({
    open: false,
  });
  const [operationLogModal, setOperationLogModal] = useState<Record<string, unknown>>({
    open: false,
  });
  const [agentBatchUpdateModal, setAgentBatchUpdateModal] = useState<Record<string, unknown>>({
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
          try {
            const response = await request({
              url: `/cluster/node/update`,
              method: 'POST',
              data: record,
            });
            if (response.success) {
              message.success(i18n.t('basic.OperatingSuccess'));
            } else {
              Modal.destroyAll();
            }
          } catch (e) {
            Modal.destroyAll();
          }
          await getList();
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
          try {
            const response = await request({
              url: `/cluster/node/update`,
              method: 'POST',
              data: {
                ...record,
                isInstall: true,
              },
            });
            if (response.success) {
              message.success(i18n.t('basic.OperatingSuccess'));
            } else {
              Modal.destroyAll();
            }
          } catch (e) {
            Modal.destroyAll();
          }
          await getList();
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
  const openOperationLogModal = ({ ip }) => {
    setOperationLogModal({ open: true, ip: ip });
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
        return result;
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
    {
      label: <Button type="link">{i18n.t('pages.GroupDetail.OperationLog')}</Button>,
      key: '5',
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
      case '5':
        openOperationLogModal(record);
        break;
      default:
        break;
    }
  };
  const agentInstallerList = useRef([]);
  const [agentVersionObj, setAgentVersionObj] = useState({});
  useEffect(() => {
    (() => {
      getModuleList().then(res => {
        agentInstallerList.current = res?.list;
        setAgentVersionObj(versionMap(res?.list));
      });
    })();
  }, [type]);
  const onOpenAgentModal = () => {
    setAgentStatusModal({ open: true });
  };
  const [nodeList, setNodeList] = useState([]);
  const statusPagination = {
    total: nodeList?.length,
  };

  const [isSmall, setIsSmall] = useState(window.innerWidth < 1600);

  useEffect(() => {
    const handleResize = () => {
      setIsSmall(window.innerWidth < 1600);
    };

    window.addEventListener('resize', handleResize);
    return () => {
      window.removeEventListener('resize', handleResize);
    };
  }, []);

  const getOperationMenu = useMemo(() => {
    return isSmall
      ? [
          {
            title: i18n.t('basic.Operating'),
            dataIndex: 'action',
            key: 'operation',
            width: isSmall ? 200 : 400,
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
        ]
      : [
          {
            title: i18n.t('basic.Operating'),
            dataIndex: 'action',
            key: 'operation',
            width: isSmall ? 200 : 400,
            render: (text, record) => (
              <>
                <Button type="link" onClick={() => onEdit(record)}>
                  {i18n.t('basic.Edit')}
                </Button>
                <Button type="link" onClick={() => onDelete(record)}>
                  {i18n.t('basic.Delete')}
                </Button>
                <Button
                  type="link"
                  onClick={() => getNodeData(record.id).then(() => setOperationType('onInstall'))}
                >
                  {i18n.t('pages.Cluster.Node.Install')}
                </Button>
                <Button
                  type="link"
                  onClick={() => getNodeData(record.id).then(() => setOperationType('onRestart'))}
                >
                  {i18n.t('pages.Nodes.Restart')}
                </Button>
                <Button
                  type="link"
                  onClick={() => getNodeData(record.id).then(() => setOperationType('onUnload'))}
                >
                  {i18n.t('pages.Cluster.Node.Unload')}
                </Button>
                <Button type="link" onClick={() => onLog(record)}>
                  {i18n.t('pages.Cluster.Node.InstallLog')}
                </Button>
                <Button type="link" onClick={() => openHeartModal(record)}>
                  {i18n.t('pages.Clusters.Node.Agent.HeartbeatDetection')}
                </Button>
                <Button type="link" onClick={() => openOperationLogModal(record)}>
                  {i18n.t('pages.GroupDetail.OperationLog')}
                </Button>
              </>
            ),
          },
        ];
  }, [isSmall]);
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
        title: i18n.t('pages.Clusters.Node.Agent.Version'),
        dataIndex: 'moduleIdList',
        render: (text, record) => {
          const index = text.slice(0, 1)[0];
          return agentVersionObj[index];
        },
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
    ].concat(getOperationMenu);
  }, [onDelete, onInstall, onUnload, type, agentVersionObj, isSmall]);

  const [disabled, setDisabled] = useState(true);
  const finalStatus = useRef(false);
  const [selectedRowKeys, setSelectedRowKeys] = useState([]);
  const [statusAgentList, setStatusAgentList] = useState([]);
  const [batchUpdateArgs, setBatchUpdateArgs] = useState({
    map: [],
    interval: 0,
    ids: [],
    operationType: 0,
    submitDataList: [],
  });
  const getBatchUpdateArgs = args => {
    setBatchUpdateArgs(args);
  };
  const rowSelection = {
    selectedRowKeys,
    onChange: (selectedRowKeys, selectedRows) => {
      setSelectedRowKeys(selectedRowKeys);
      setDisabled(selectedRowKeys.length <= 0);
      setNodeList(selectedRows);
    },
  };
  const onUpdate = async submitData =>
    await request({
      url: '/cluster/node/update',
      method: 'POST',
      data: submitData,
    });
  const getAgentNode = async id =>
    await request({
      url: `/cluster/node/get/${id}`,
      method: 'GET',
    });
  const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
  const batchUpdate = async () => {
    const { map, interval, submitDataList } = batchUpdateArgs;
    setStatusAgentList(submitDataList);
    const entries = Array.from(map.entries());
    for (let i = 0; i < entries.length; i++) {
      const [key, value] = entries[i];
      for (let j = 0; j < value.length; j++) {
        const agentNode = await getAgentNode(value[j]?.id);
        if (i === entries.length - 1 && j === value.length - 1) {
          onUpdate({
            ...value[j],
            version: agentNode.version,
            agentRestartTime:
              batchUpdateArgs.operationType === 3
                ? value[j].agentRestartTime + 1
                : value[j].agentRestartTime,
          });
          await delay(1000);
          await closeStatusModal();
          setStatusAgentList([]);
        } else {
          onUpdate({
            ...value[j],
            version: agentNode.version,
            agentRestartTime:
              batchUpdateArgs.operationType === 3
                ? value[j].agentRestartTime + 1
                : value[j].agentRestartTime,
          });
        }
      }
      if (i < entries.length - 1) {
        finalStatus.current = false;
        await delay(interval * 60 * 1000);
      }
    }
    new Promise(resolve => {
      finalStatus.current = true;
    });
  };
  const getStatusAgentList = option => {
    const { ids } = batchUpdateArgs;
    request({
      url: '/cluster/node/list',
      method: 'POST',
      data: { ...option },
    }).then(response => {
      const list = response?.list.filter(item => ids.includes(item.id));
      setStatusAgentList(list);
    });
  };

  useEffect(() => {
    batchUpdate();
  }, [batchUpdateArgs]);
  const [agentStatusModal, setAgentStatusModal] = useState({
    open: false,
  });

  useEffect(() => {
    const internalId = setInterval(() => {
      if (agentStatusModal.open) {
        getStatusAgentList({
          pageSize: 99999,
          pageNum: 1,
          type: 'AGENT',
          parentId: +clusterId,
        });
      }
    }, 10000);
    return () => {
      clearInterval(internalId);
    };
  }, [agentStatusModal.open]);

  const closeStatusModal = async () => {
    const completeList = statusAgentList.filter(item => item.status !== 2);
    if (completeList.length === statusAgentList.length && finalStatus.current) {
      setStatusAgentList([]);
    }
    setNodeList([]);
    setSelectedRowKeys([]);
    setDisabled(true);
    await getList();
    setAgentStatusModal({ open: false });
  };
  const tableProps = { rowSelection: {} };
  if (type === 'AGENT') {
    tableProps.rowSelection = {
      type: 'checkbox',
      ...rowSelection,
    };
  } else {
    delete tableProps.rowSelection;
  }
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
          form: form,
          onFilter,
        }}
        suffix={
          <>
            {type === 'AGENT' && (
              <Button
                type="primary"
                disabled={disabled}
                onClick={() =>
                  setAgentBatchUpdateModal({
                    open: true,
                    agentList: nodeList,
                    openStatusModal: onOpenAgentModal,
                    getArgs: getBatchUpdateArgs,
                  })
                }
              >
                {i18n.t('pages.Clusters.Node.BatchUpdate')}
              </Button>
            )}
            {statusAgentList.length > 0 && (
              <Button
                type="primary"
                onClick={() =>
                  setAgentStatusModal({
                    open: true,
                  })
                }
              >
                {i18n.t('pages.Clusters.Node.OperationStatusQuery')}
              </Button>
            )}
            <Button
              type="primary"
              onClick={() =>
                setNodeEditModal({ open: true, agentInstallerList: agentInstallerList.current })
              }
            >
              {i18n.t('pages.Clusters.Node.Create')}
            </Button>
          </>
        }
        table={{
          ...tableProps,
          columns:
            type === 'AGENT'
              ? columns.filter(
                  item =>
                    item.dataIndex !== 'enabledOnline' &&
                    item.dataIndex !== 'port' &&
                    item.dataIndex !== 'protocolType' &&
                    item.dataIndex !== 'modifier',
                )
              : columns.filter(
                  item => item.dataIndex !== 'moduleIdList' && item.dataIndex !== 'installer',
                ),
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
        onCancel={() => setHeartModal({ open: false })}
      />
      <AgentBatchUpdateModal
        {...agentBatchUpdateModal}
        open={agentBatchUpdateModal.open as boolean}
        onOk={() => {
          setAgentBatchUpdateModal({ open: false });
        }}
        onCancel={() => setAgentBatchUpdateModal({ open: false })}
      />

      <OperationLogModal
        {...operationLogModal}
        onOk={() => {
          setOperationLogModal({ open: false });
        }}
        onCancel={() => setOperationLogModal({ open: false })}
      ></OperationLogModal>
      <Modal
        open={agentStatusModal.open}
        title={i18n.t('basic.Status')}
        width={1400}
        footer={[
          <Button
            key="cancel"
            onClick={async () => {
              await closeStatusModal();
            }}
          >
            {i18n.t('pages.GroupDetail.Stream.Closed')}
          </Button>,
        ]}
      >
        <HighTable
          table={{
            columns: columns.filter(
              item =>
                item.dataIndex !== 'action' &&
                item.dataIndex !== 'enabledOnline' &&
                item.dataIndex !== 'port' &&
                item.dataIndex !== 'protocolType',
            ),
            rowKey: 'id',
            dataSource: statusAgentList,
            pagination: statusPagination,
          }}
        />
      </Modal>
    </PageContainer>
  );
};

export default Comp;
