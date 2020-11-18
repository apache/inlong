import React, { ReactNode, useContext, useState } from 'react';
import GlobalContext from '@/context/globalContext';
import Breadcrumb from '@/components/Breadcrumb';
import Table from '@/components/Tablex';
import TitleWrap from '@/components/TitleWrap';
import { Form, Button, Spin, Col, Row, Switch, Tabs } from 'antd';
import { useImmer } from 'use-immer';
import './index.less';
import { useRequest } from '@/hooks';
import { useParams } from 'react-router-dom';
import { boolean2Chinese, transParamsWithConstantsMap } from '@/utils';
import { BROKER_INFO_ZH_MAP } from '@/constants/broker';
import tableFilterHelper from '@/components/Tablex/tableFilterHelper';
import CommonModal, { OPTIONS, onOpenModal, BrokerData } from './commonModal';

declare type BrokerQueryData = {
  withDetail: boolean;
  brokerId: string;
};

declare type TopicQueryData = {
  withTopic: boolean;
  brokerId: string;
};

const { TabPane } = Tabs;

const Detail: React.FC = () => {
  const { id } = useParams();
  const { breadMap } = useContext(GlobalContext);
  const [form] = Form.useForm();
  const [modalParams, updateModelParams] = useImmer<any>({});
  const [acceptPublish, setAcceptPublish] = useState<any>(false);
  const [acceptSubscribe, setAcceptSubscribe] = useState<any>(false);
  const [filterData, updateFilterData] = useImmer<any>({});
  const queryBrokerConf = useRequest<any>(
    (
      data: BrokerQueryData = {
        withDetail: true,
        brokerId: id,
      }
    ) => ({
      url: '/api/op_query/admin_query_broker_run_status',
      data: {
        ...data,
      },
    }),
    {
      onSuccess: data => {
        setAcceptPublish(data[0]['acceptPublish'] === 'true');
        setAcceptSubscribe(data[0]['acceptSubscribe'] === 'true');
      },
    }
  );
  const queryTopicInfo = useRequest<any>(
    (
      data: TopicQueryData = {
        withTopic: true,
        brokerId: id,
      }
    ) => ({
      url: '/api/op_query/admin_query_broker_configure',
      data: {
        ...data,
      },
    })
  );

  // render
  const renderConf = () => {
    const columns = [
      {
        title: '类别',
        dataIndex: `type`,
      },
      {
        title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'acceptPublish'),
        dataIndex: 'acceptPublish',
        render: (t: string) => boolean2Chinese(t),
      },
      {
        title: transParamsWithConstantsMap(
          BROKER_INFO_ZH_MAP,
          'acceptSubscribe'
        ),
        dataIndex: 'acceptSubscribe',
        render: (t: string) => boolean2Chinese(t),
      },
      {
        title: transParamsWithConstantsMap(
          BROKER_INFO_ZH_MAP,
          'unflushThreshold'
        ),
        dataIndex: 'unflushThreshold',
      },
      {
        title: transParamsWithConstantsMap(
          BROKER_INFO_ZH_MAP,
          'unflushInterval'
        ),
        dataIndex: 'unflushInterval',
      },
      {
        title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'deleteWhen'),
        dataIndex: 'deleteWhen',
      },
      {
        title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'deletePolicy'),
        dataIndex: 'deletePolicy',
      },
      {
        title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'numPartitions'),
        dataIndex: 'numPartitions',
      },
      {
        title: '操作',
        render: (t: string, r: BrokerData) => {
          return <a onClick={() => onEditConf(r)}>编辑</a>;
        },
      },
    ];
    const { data } = queryBrokerConf;
    if (!data || !data[0]) return null;
    const { BrokerSyncStatusInfo } = data[0];
    const dataSource = [];
    dataSource.push({
      type: '缺省配置',
      ...BrokerSyncStatusInfo.curBrokerDefaultConfInfo,
    });
    dataSource.push({
      type: '最近上报',
      ...BrokerSyncStatusInfo.reportedBrokerDefaultConfInfo,
    });
    dataSource.push({
      type: '最近下发',
      ...BrokerSyncStatusInfo.lastPushBrokerDefaultConfInfo,
    });

    return <Table columns={columns} dataSource={dataSource} rowKey="type" />;
  };
  const renderTopics = (type: string): ReactNode => {
    const columns = [
      {
        title: 'topicName',
        dataIndex: `topicName`,
      },
      {
        title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'numPartitions'),
        dataIndex: 'numPartitions',
      },
      {
        title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'acceptPublish'),
        dataIndex: 'acceptPublish',
        render: (t: string) => boolean2Chinese(t),
      },
      {
        title: transParamsWithConstantsMap(
          BROKER_INFO_ZH_MAP,
          'acceptSubscribe'
        ),
        dataIndex: 'acceptSubscribe',
        render: (t: string) => boolean2Chinese(t),
      },
      {
        title: transParamsWithConstantsMap(
          BROKER_INFO_ZH_MAP,
          'unflushThreshold'
        ),
        dataIndex: 'unflushThreshold',
      },
      {
        title: transParamsWithConstantsMap(
          BROKER_INFO_ZH_MAP,
          'unflushInterval'
        ),
        dataIndex: 'unflushInterval',
      },
      {
        title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'deleteWhen'),
        dataIndex: 'deleteWhen',
      },
      {
        title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'deletePolicy'),
        dataIndex: 'deletePolicy',
      },
    ];
    const { data } = queryBrokerConf;
    if (!data || !data[0]) return null;
    const { BrokerSyncStatusInfo } = data[0];
    let dataSource: any[] = [];
    if (type === 'cur') {
      dataSource = BrokerSyncStatusInfo.curBrokerTopicSetConfInfo;
    } else if (type === 'lastPush') {
      dataSource = BrokerSyncStatusInfo.lastPushBrokerTopicSetConfInfo;
    } else if (type === 'lastReported') {
      dataSource = BrokerSyncStatusInfo.reportedBrokerTopicSetConfInfo;
    }

    return (
      <Table
        columns={columns}
        dataSource={dataSource}
        rowKey={r => type + r.topicName}
        dataSourceX={filterData.list}
        searchPlaceholder="请输入TopicName搜索"
        searchStyle={{
          position: 'absolute',
          top: '-55px',
          right: '10px',
          zIndex: 1,
        }}
        filterFnX={value =>
          tableFilterHelper({
            key: value,
            srcArray: dataSource,
            targetArray: filterData.list,
            updateFunction: res =>
              updateFilterData(filterData => {
                filterData.list = res;
              }),
            filterList: ['topicName'],
          })
        }
      />
    );
  };

  // event
  // acceptPublish && acceptSubscribe event
  const onSwitchChange = (e: boolean, type: string) => {
    let option = '';
    if (type === 'acceptPublish') {
      option = e ? '发布' : '禁止可发布';
    } else if (type === 'acceptSubscribe') {
      option = e ? '订阅' : '禁止可订阅';
    }

    onOpenModal({
      type: 'brokerStateChange',
      title: `请确认操作`,
      updateFunction: updateModelParams,
      params: {
        option,
        id: queryBrokerConf.data[0].brokerId,
        callback: () => {
          if (type === 'acceptPublish') {
            setAcceptPublish(e);
          } else if (type === 'acceptSubscribe') {
            setAcceptSubscribe(e);
          }
        },
      },
    });
  };

  const onOptions = (type: string) => {
    onOpenModal({
      type,
      title: `确认进行【${OPTIONS.find(t => t.value === type)?.name}】操作？`,
      updateFunction: updateModelParams,
      params: [queryBrokerConf.data[0].brokerId],
    });
  };

  // new broker
  const onEditConf = (r: BrokerData) => {
    onOpenModal({
      type: 'editBroker',
      title: '编辑Broker',
      updateFunction: updateModelParams,
      params: r,
    });
  };

  return (
    <Spin spinning={queryBrokerConf.loading && queryTopicInfo.loading}>
      <Breadcrumb
        breadcrumbMap={breadMap}
        appendParams={`Broker（${id}）详情`}
      />
      <div className="main-container">
        <TitleWrap title="运行状态" wrapperStyle={{ position: 'relative' }}>
          <div className="broker-detail-options-wrapper">
            <Switch
              className="mr10"
              checked={acceptPublish}
              checkedChildren="订阅"
              unCheckedChildren="订阅"
              onChange={e => onSwitchChange(e, 'acceptPublish')}
            />
            <Switch
              className="mr10"
              checked={acceptSubscribe}
              checkedChildren="发布"
              unCheckedChildren="发布"
              onChange={e => onSwitchChange(e, 'acceptSubscribe')}
            />
            <Button
              className="mr10"
              type="primary"
              size="small"
              onClick={() => onOptions('online')}
            >
              上线
            </Button>
            <Button
              className="mr10"
              type="primary"
              size="small"
              onClick={() => onOptions('offline')}
            >
              下线
            </Button>
            <Button
              className="mr10"
              type="primary"
              size="small"
              onClick={() => onOptions('reload')}
            >
              重载
            </Button>
          </div>
          <Form form={form}>
            <Row gutter={24}>
              {queryBrokerConf.data &&
                Object.keys(queryBrokerConf.data[0]).map(
                  (t: string, index: number) => {
                    const label = transParamsWithConstantsMap(
                      BROKER_INFO_ZH_MAP,
                      t
                    );
                    const ignoreList = [
                      'acceptPublish',
                      'brokerVersion',
                      'acceptSubscribe',
                    ];
                    if (
                      queryBrokerConf.data[0][t] instanceof Object ||
                      !label ||
                      ignoreList.includes(t)
                    )
                      return null;
                    return (
                      <Col span={12} key={'queryBrokerConf' + index}>
                        <Form.Item labelCol={{ span: 12 }} label={label}>
                          {queryBrokerConf.data[0][t] + ''}
                        </Form.Item>
                      </Col>
                    );
                  }
                )}
            </Row>
          </Form>
        </TitleWrap>
        <TitleWrap title="缺省配置">{renderConf()}</TitleWrap>
        <TitleWrap title="Topic集合配置">
          <Tabs>
            <TabPane tab="当前配置" key="cur">
              {renderTopics('cur')}
            </TabPane>
            <TabPane tab="最后下发" key="lastPush">
              {renderTopics('lastPush')}
            </TabPane>
            <TabPane tab="最后上报" key="lastReported">
              {renderTopics('lastReported')}
            </TabPane>
          </Tabs>
        </TitleWrap>
      </div>
      <CommonModal
        modalParams={modalParams}
        data={[queryBrokerConf.data && queryBrokerConf.data[0]]}
      />
    </Spin>
  );
};

export default Detail;
