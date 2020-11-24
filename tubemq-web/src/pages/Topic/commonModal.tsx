import { boolean2Chinese } from '@/utils';
import Table from '@/components/Tablex';
import { Col, Form, Input, message, Row } from 'antd';
import Modal, { OKProps } from '@/components/Modalx';
import React from 'react';
import Query from '@/pages/Topic/query';
import { FormProps } from 'antd/lib/form';

export const OPTIONS = [
  {
    value: 'delete',
    name: '删除',
  },
];
export const OPTIONS_VALUES = OPTIONS.map(t => t.value);

// interface
export declare type TopicData = any[];
export interface TopicResultData {
  topicName: string;
  infoCount: string;
  totalCfgNumPart: string;
  totalRunNumPartCount: string;
  isSrvAcceptPublish: string | number;
  isSrvAcceptSubscribe: string | number;
  enableAuthControl: string | number;
  [key: string]: any;
}
export interface TopicModalProps {
  type: string;
  title?: string;
  updateFunction: (draft: any) => any;
  params?: any;
}
interface ComProps {
  modalParams: any;
  data: any[];
}
// exports broker modal
// render funcs
const renderTopicOptions = (modalParams: any, dataSource: any[]) => {
  const columns = [
    {
      title: 'Topic',
      render: (t: string, r: TopicResultData) => {
        return `${r.brokerId}#${r.brokerIp}:${r.brokerPort}`;
      },
    },
    {
      title: 'TopicIP',
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
const renderNewTopic = (form: any) => {
  const brokerFormArr = [
    {
      name: 'topicName',
      defaultValue: '',
    },
    {
      name: 'numPartitions',
      defaultValue: '3',
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
const renderChooseBroker = (modalParams: any) => {
  const { params } = modalParams;
  const columns = [
    {
      title: 'Broker',
      render: (t: string, r: TopicResultData) => {
        return `${r.brokerId}#${r.brokerIp}:${r.brokerPort}`;
      },
    },
    {
      title: '实例数',
      dataIndex: ['runInfo', 'numTopicStores'],
    },
    {
      title: '当前运行状态',
      dataIndex: ['runInfo', 'brokerManageStatus'],
    },
    {
      title: '可发布',
      dataIndex: ['runInfo', 'acceptPublish'],
      render: (t: string) => boolean2Chinese(t),
    },
    {
      title: '可订阅',
      dataIndex: ['runInfo', 'acceptSubscribe'],
      render: (t: string) => boolean2Chinese(t),
    },
  ];
  const onChangeSelect = (p: any) => {
    selectBroker = p;
  };
  return (
    <Table
      rowSelection={{ onChange: onChangeSelect }}
      columns={columns}
      dataSource={params.data}
      rowKey="brokerId"
    />
  );
};
const renderEditTopic = (modalParams: any, form: FormProps['form']) => {
  const { params: p } = modalParams;
  const pickArr = [
    'topicName',
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
const renderTopicStateChange = (modalParams: any) => {
  const { params } = modalParams;

  return (
    <div>
      请确认<span className="enhance">{params.option}</span> 以下broker列表的
      topic :<span className="enhance">（{params.topicName}）</span> 的 Topic?
      {renderChooseBroker(modalParams)}
    </div>
  );
};
const renderDeleteTopic = (modalParams: any) => {
  const { params } = modalParams;

  return (
    <div>
      请确认<span className="enhance">删除</span> 以下broker列表的 topic :
      <span className="enhance">（{params.topicName}）</span> 吗?
      {renderChooseBroker(modalParams)}
    </div>
  );
};
const renderDeleteConsumeGroup = (modalParams: any) => {
  const { params } = modalParams;

  return (
    <div>
      确认<span className="enhance">删除</span> 以下 :
      <span className="enhance">（{params.groupName}）</span> 吗?
    </div>
  );
};
const renderAuthorizeControlChange = (modalParams: any) => {
  const { params } = modalParams;

  return (
    <div>
      请确认
      <span className="enhance">
        {params.value ? '启动' : '关闭'}topic
        <span className="enhance">（{params.topicName}）</span>的消费组授权控制
      </span>
      吗？
    </div>
  );
};
export const onOpenModal = (p: TopicModalProps) => {
  const { type, title, updateFunction, params } = p;
  updateFunction((m: any) => {
    m.type = type;
    m.params = params;
    Object.assign(m, {
      params,
      visible: type,
      title,
      onOk: (p: OKProps) => {
        updateFunction((m: any) => {
          if (type === 'newTopic' || type === 'editTopic') {
            p.params = Object.assign(f && f.getFieldsValue(), {
              callback: p.params.callback,
            });
          }

          if (
            type === 'chooseBroker' ||
            type === 'topicStateChange' ||
            type === 'deleteTopic'
          ) {
            if (!selectBroker.length) {
              message.error('至少选择一列！');
              return;
            }

            // end
            if (type === 'chooseBroker') {
              m.query =
                p.params.subType === 'edit'
                  ? 'endEditChooseBroker'
                  : 'endChooseBroker';
            }
            p.params = Object.assign({}, p.params, {
              selectBroker,
            });
          }

          m.okParams = p;
          m.isOk = Date.now();
        });
      },
      onCancel: () =>
        updateFunction((m: any) => {
          m.visible = false;
          m.isOk = null;
        }),
    });
  });
};

let selectBroker: any[] = [];
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
          renderTopicOptions(
            modalParams,
            data.filter((t: TopicResultData) =>
              modalParams.params.includes(t.brokerId)
            )
          )}
        {modalParams.type === 'newTopic' && renderNewTopic(form)}
        {modalParams.type === 'chooseBroker' && renderChooseBroker(modalParams)}
        {modalParams.type === 'editTopic' && renderEditTopic(modalParams, form)}
        {modalParams.type === 'topicStateChange' &&
          renderTopicStateChange(modalParams)}
        {modalParams.type === 'deleteTopic' && renderDeleteTopic(modalParams)}
        {modalParams.type === 'deleteConsumeGroup' &&
          renderDeleteConsumeGroup(modalParams)}
        {modalParams.type === 'authorizeControl' &&
          renderAuthorizeControlChange(modalParams)}
      </div>
      <Query
        fire={modalParams.isOk}
        params={modalParams.okParams}
        type={modalParams.visible && (modalParams.query || modalParams.type)}
      />
    </Modal>
  );
};

export default Comp;
