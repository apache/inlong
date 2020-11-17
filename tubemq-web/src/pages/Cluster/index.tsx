import React, { useContext } from 'react';
import GlobalContext from '@/context/globalContext';
import Breadcrumb from '@/components/Breadcrumb';
import Table from '@/components/Tablex';
import { Spin } from 'antd';
import './index.less';
import { useRequest } from '@/hooks';
import Modal, { OKProps } from '@/components/Modalx';
import { useImmer } from 'use-immer';

interface ClusterResultData {
  groupName: string;
  groupStatus: string;
  hostName: string;
  index: number;
  port: string;
  nodeStatus: string;
  length: number;
}

const queryClusterList = (data: ClusterResultData) => ({
  url: '/api/op_query/admin_query_master_group_info',
  data: data,
});

const Cluster: React.FC = () => {
  const { breadMap } = useContext(GlobalContext);
  const [modalParams, updateModelParams] = useImmer<any>({
    title: '请确认操作',
  });
  const { data, loading } = useRequest<any, any>(queryClusterList, {
    formatResult: d => {
      return {
        list: d.data.map((t: any) => ({
          groupName: d.groupName,
          groupStatus: d.groupStatus,
          hostName: t.hostName,
          index: t.index,
          port: t.port,
          nodeStatus: t.statusInfo.nodeStatus,
          length: d.data.length,
        })),
      };
    },
  });
  const columns = [
    {
      title: '集群名',
      dataIndex: 'groupName',
      render: (t: string, r: ClusterResultData, index: number) => {
        return {
          children: t,
          props: {
            rowSpan: index === 0 ? r.length : 0,
          },
        };
      },
    },
    {
      title: '集群状态',
      dataIndex: 'groupStatus',
      render: (t: string, r: ClusterResultData, index: number) => {
        return {
          children: t,
          props: {
            rowSpan: index === 0 ? r.length : 0,
          },
        };
      },
    },
    {
      title: '节点名',
      render: (t: string, r: ClusterResultData) => {
        return `${r.groupName}-${r.hostName}`;
      },
    },
    {
      title: 'IP地址',
      render: (t: string, r: ClusterResultData) => {
        return `${r.hostName}-${r.port}`;
      },
    },
    {
      title: '节点名',
      dataIndex: 'nodeStatus',
    },
    {
      title: '操作',
      render: (t: string, r: ClusterResultData, index: number) => {
        return {
          children: (
            <span className="options-wrapper">
              <a onClick={() => onSwitchCluster(t, r)}>切换</a>
            </span>
          ),
          props: {
            rowSpan: index === 0 ? r.length : 0,
          },
        };
      },
    },
  ];

  const switchClusterQuery = useRequest<any, any>(
    (data?: ClusterResultData) => ({
      url: '/api/op_modify/admin_transfer_current_master',
      data,
    }),
    { manual: true }
  );
  const onSwitchCluster = (t: string, r: ClusterResultData) => {
    updateModelParams(d => {
      d = Object.assign(d, {
        visible: true,
        onOk: (p: OKProps) => {
          switchClusterQuery.run({
            confModAuthToken: p.psw,
          });
        },
        onCancel: () => {
          updateModelParams((m: any) => {
            m.visible = false;
          });
        },
      });
    });
  };
  return (
    <Spin spinning={loading}>
      <Breadcrumb breadcrumbMap={breadMap}></Breadcrumb>
      <div className="main-container">
        <Table columns={columns} dataSource={data?.list} rowKey="index"></Table>
      </div>
      <Modal {...modalParams}>
        <div>
          确认<span className="enhance">切换</span>集群?
        </div>
      </Modal>
    </Spin>
  );
};

export default Cluster;
