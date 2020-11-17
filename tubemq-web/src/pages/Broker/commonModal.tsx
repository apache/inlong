import { boolean2Chinese } from '@/utils';
import Table from '@/components/Tablex';
import { Col, Form, Input, Row } from 'antd';
import Modal, { OKProps } from '@/components/Modalx';
import React from 'react';
import Query from '@/pages/Broker/query';
import { FormProps } from 'antd/lib/form';

export const OPTIONS = [
  {
    value: 'online',
    name: '上线',
  },
  {
    value: 'offline',
    name: '下线',
  },
  {
    value: 'reload',
    name: '重载',
  },
  {
    value: 'delete',
    name: '删除',
  },
];
export const OPTIONS_VALUES = OPTIONS.map(t => t.value);

// interface
export declare type BrokerData = any[];
export interface BrokerResultData {
  acceptPublish: string;
  acceptSubscribe: string;
  brokerId: number;
  brokerIp: string;
  brokerPort: number;
  brokerTLSPort: number;
  brokerVersion: string;
  enableTLS: boolean;
  isAutoForbidden: boolean;
  isBrokerOnline: string;
  isConfChanged: string;
  isConfLoaded: string;
  isRepAbnormal: boolean;
  manageStatus: string;
  runStatus: string;
  subStatus: string;
  [key: string]: any;
}
export interface BrokerModalProps {
  type: string;
  title: string;
  updateFunction: (draft: any) => any;
  params?: any;
}
// exports broker modal
// render funcs
const renderBrokerOptions = (modalParams: any, dataSource: any[]) => {
  const columns = [
    {
      title: 'Broker',
      render: (t: string, r: BrokerResultData) => {
        return `${r.brokerId}#${r.brokerIp}:${r.brokerPort}`;
      },
    },
    {
      title: 'BrokerIP',
      dataIndex: 'brokerIp',
    },
    {
      title: '管理状态',
      dataIndex: 'manageStatus',
    },
    {
      title: '运行状态',
      dataIndex: 'runStatus',
    },
    {
      title: '运行子状态',
      dataIndex: 'subStatus',
    },
    {
      title: '可发布',
      render: (t: string) => boolean2Chinese(t),
    },
    {
      title: '可订阅',
      render: (t: string) => boolean2Chinese(t),
    },
  ];
  return <Table columns={columns} dataSource={dataSource} rowKey="brokerId" />;
};
const renderNewBroker = (form: any) => {
  const brokerFormArr = [
    {
      name: 'brokerId',
      defaultValue: '0',
    },
    {
      name: 'numPartitions',
      defaultValue: '3',
    },
    {
      name: 'brokerIp',
      defaultValue: '',
    },
    {
      name: 'brokerPort',
      defaultValue: '8123',
    },
    {
      name: 'deleteWhen',
      defaultValue: '0 0 6,18 * * ?',
    },
    {
      name: 'deletePolicy',
      defaultValue: 'delete,168h',
    },
    {
      name: 'unflushThreshold',
      defaultValue: '1000',
    },
    {
      name: 'unflushInterval',
      defaultValue: '10000',
    },
    {
      name: 'acceptPublish',
      defaultValue: 'true',
    },
    {
      name: 'acceptSubscribe',
      defaultValue: 'true',
    },
  ];

  return (
    <Form form={form}>
      <Row gutter={24}>
        {brokerFormArr.map((t, index) => (
          <Col span={12} key={'brokerFormArr' + index}>
            <Form.Item
              labelCol={{ span: 12 }}
              label={t.name}
              name={t.name}
              initialValue={t.defaultValue}
            >
              <Input />
            </Form.Item>
          </Col>
        ))}
      </Row>
    </Form>
  );
};
const renderEditBroker = (modalParams: any, form: FormProps['form']) => {
  const { params: p } = modalParams;
  const pickArr = [
    'numPartitions',
    'unflushThreshold',
    'unflushInterval',
    'deleteWhen',
    'deletePolicy',
    'acceptPublish',
    'acceptSubscribe',
  ];
  const brokerFormArr: Array<{
    name: string;
    defaultValue: string;
  }> = [];
  pickArr.forEach(t => {
    brokerFormArr.push({
      name: t,
      defaultValue: p[t],
    });
  });

  return (
    <Form form={form}>
      <Row gutter={24}>
        {brokerFormArr.map((t, index) => (
          <Col span={12} key={'brokerFormArr' + index}>
            <Form.Item
              labelCol={{ span: 12 }}
              label={t.name}
              name={t.name}
              initialValue={t.defaultValue}
            >
              <Input />
            </Form.Item>
          </Col>
        ))}
      </Row>
    </Form>
  );
};
const renderBrokerStateChange = (modalParams: any) => {
  const { params } = modalParams;

  return (
    <div>
      请确认<span className="enhance">{params.option}</span> ID:{' '}
      <span className="enhance">{params.id}</span> 的 Broker?
    </div>
  );
};
export const onOpenModal = (p: BrokerModalProps) => {
  const { type, title, updateFunction, params } = p;
  if (typeof params === 'function') {
    p.params = params();
  }
  updateFunction((m: any) => {
    m.type = type;
    m.params = params;
    Object.assign(m, {
      params,
      visible: type,
      title,
      onOk: (p: OKProps) => {
        updateFunction((m: any) => {
          if (type === 'newBroker' || type === 'editBroker') {
            p.params = f && f.getFieldsValue();
          }
          m.okParams = p;
          m.isOk = Date.now();
        });
      },
      onCancel: () => {
        updateFunction((m: any) => {
          m.visible = false;
          m.isOk = null;
        });
      },
    });
  });
};

interface ComProps {
  modalParams: any;
  data: any[];
}
let f: FormProps['form'];
const Comp = (props: ComProps) => {
  const { modalParams, data } = props;
  const [form] = Form.useForm();
  f = form;

  return (
    <Modal {...modalParams}>
      <div>
        {modalParams.type &&
          OPTIONS_VALUES.includes(modalParams.type) &&
          renderBrokerOptions(
            modalParams,
            data.filter((t: BrokerResultData) =>
              modalParams.params.includes(t.brokerId)
            )
          )}
        {modalParams.type === 'newBroker' && renderNewBroker(form)}
        {modalParams.type === 'editBroker' &&
          renderEditBroker(modalParams, form)}
        {modalParams.type === 'brokerStateChange' &&
          renderBrokerStateChange(modalParams)}
      </div>
      <Query
        fire={modalParams.isOk}
        params={modalParams.okParams}
        type={modalParams.query || modalParams.type}
      />
    </Modal>
  );
};

export default Comp;
