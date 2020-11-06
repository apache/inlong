import React, { ReactNode, useContext, useState } from 'react';
import GlobalContext from '@/context/globalContext';
import Breadcrumb from '@/components/Breadcrumb';
import Table from '@/components/Tablex';
import TitleWrap from '@/components/TitleWrap';
import { Form, Button, Spin, Col, Row, Switch } from 'antd';
import { useImmer } from 'use-immer';
import './index.less';
import { useRequest } from '@/hooks';
import { useParams } from 'react-router-dom';
import { boolean2Chinese, transParamsWithConstantsMap } from '@/utils';
import tableFilterHelper from '@/components/Tablex/tableFilterHelper';
import CommonModal, { onOpenModal, TopicResultData } from './commonModal';
import BrokerModal, {
  onOpenModal as onOpenBrokerModal,
} from '@/pages/Broker/commonModal';
import { BROKER_INFO_ZH_MAP } from '@/constants/broker';
import { PERSON_INFO_ZH_MAP } from '@/constants/person';
import { TOPIC_INFO_ZH_MAP } from '@/constants/topic';

declare type TopicQueryData = {
  topicName: string;
};

const Detail: React.FC = () => {
  const { name } = useParams();
  const { breadMap } = useContext(GlobalContext);
  const [form] = Form.useForm();
  const [modalParams, updateModelParams] = useImmer<any>({});
  const [brokerModalParams, updateBrokerModalParams] = useImmer<any>({});
  const [isSrvAcceptPublish, setIsSrvAcceptPublish] = useState<any>(false);
  const [isSrvAcceptSubscribe, setIsSrvAcceptSubscribe] = useState<any>(false);
  const [enableAuthControl, setEnableAuthControl] = useState<any>(false);
  const [filterData, updateFilterData] = useImmer<any>({});
  const queryTopicInfo = useRequest<any>(
    (
      data: TopicQueryData = {
        topicName: name,
      }
    ) => ({
      url: '/api/op_query/admin_query_topic_authorize_control',
      data: {
        ...data,
      },
    })
  );
  const queryTopicConf = useRequest<any>(
    (
      data: TopicQueryData = {
        topicName: name,
      }
    ) => ({
      url: '/api/op_query/admin_query_topic_info',
      data: {
        ...data,
      },
    }),
    {
      onSuccess: data => {
        setIsSrvAcceptPublish(data[0]['isSrvAcceptPublish']);
        setIsSrvAcceptSubscribe(data[0]['isSrvAcceptSubscribe']);
        setEnableAuthControl(data[0]['authData']['enableAuthControl']);
      },
    }
  );

  // render
  const searchStyle = {
    position: 'absolute',
    top: '-40px',
    right: '10px',
    zIndex: 1,
    width: '300px',
  };
  const renderBrokerList = (): ReactNode => {
    const columns = [
      {
        title: 'Broker',
        render: (t: string, r: TopicResultData) => {
          return `${r.brokerId}#${r.brokerIp}:${r.brokerPort}`;
        },
      },
      {
        title: transParamsWithConstantsMap(
          BROKER_INFO_ZH_MAP,
          'runInfo.acceptPublish'
        ),
        dataIndex: ['runInfo', 'acceptPublish'],
        render: (t: string) => boolean2Chinese(t),
      },
      {
        title: transParamsWithConstantsMap(
          BROKER_INFO_ZH_MAP,
          'runInfo.acceptSubscribe'
        ),
        dataIndex: ['runInfo', 'acceptSubscribe'],
        render: (t: string) => boolean2Chinese(t),
      },
      {
        title: transParamsWithConstantsMap(
          BROKER_INFO_ZH_MAP,
          'runInfo.numPartitions'
        ),
        dataIndex: ['runInfo', 'numPartitions'],
      },
      {
        title: transParamsWithConstantsMap(
          BROKER_INFO_ZH_MAP,
          'runInfo.brokerManageStatus'
        ),
        dataIndex: ['runInfo', 'brokerManageStatus'],
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
        title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'numPartitions'),
        dataIndex: 'numPartitions',
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
        title: '操作',
        render: (t: string, r: TopicResultData) => {
          return (
            <span>
              <a onClick={() => onEdit(r)}>编辑</a>
              <a onClick={() => onReload(r)}>重载</a>
              <a onClick={() => onDeleteBroker(r)}>删除</a>
            </span>
          );
        },
      },
    ];
    const { data } = queryTopicConf;
    if (!data || !data[0]) return null;
    const { topicInfo } = data[0];

    return (
      <Table
        columns={columns}
        dataSource={topicInfo}
        rowKey={r => `${r.brokerId}#${r.brokerIp}:${r.brokerPort}`}
        dataSourceX={filterData.topicInfoList}
        searchPlaceholder="请输入brokerId,Ip,Port搜索"
        searchStyle={searchStyle}
        filterFnX={value =>
          tableFilterHelper({
            key: value,
            srcArray: topicInfo,
            targetArray: filterData.topicInfoList,
            updateFunction: res =>
              updateFilterData(filterData => {
                filterData.topicInfoList = res;
              }),
            filterList: ['brokerId', 'brokerIp', 'brokerPort'],
          })
        }
      />
    );
  };
  const renderConsumeGroupList = (): ReactNode => {
    const columns = [
      {
        title: '消费组',
        dataIndex: 'groupName',
      },
      {
        title: transParamsWithConstantsMap(PERSON_INFO_ZH_MAP, 'createUser'),
        dataIndex: 'createUser',
      },
      {
        title: transParamsWithConstantsMap(PERSON_INFO_ZH_MAP, 'createDate'),
        dataIndex: 'createDate',
      },
      {
        title: '操作',
        render: (t: string, r: TopicResultData) => {
          return (
            <span>
              <a onClick={() => onDeleteConsumeGroup(r)}>删除</a>
            </span>
          );
        },
      },
    ];
    const { data } = queryTopicInfo;
    if (!data || !data[0]) return null;
    const { authConsumeGroup } = data[0];

    return (
      <Table
        columns={columns}
        dataSource={authConsumeGroup}
        rowKey={r => `${r.brokerId}#${r.brokerIp}:${r.brokerPort}`}
        dataSourceX={filterData.list}
        searchPlaceholder="请输入消费组名称搜索"
        searchStyle={searchStyle}
        filterFnX={value =>
          tableFilterHelper({
            key: value,
            srcArray: authConsumeGroup,
            targetArray: filterData.list,
            updateFunction: res =>
              updateFilterData(filterData => {
                filterData.list = res;
              }),
            filterList: ['groupName'],
          })
        }
      />
    );
  };

  // event
  // isSrvAcceptPublish && isSrvAcceptSubscribe event
  const queryBrokerListByTopicNameQuery = useRequest<any, any>(
    data => ({ url: '/api/op_query/admin_query_topic_info', ...data }),
    { manual: true }
  );
  const onSwitchChange = (e: boolean, type: string) => {
    let option = '';
    const topicName = queryTopicConf.data[0].topicInfo[0].topicName;
    if (type === 'isSrvAcceptPublish') {
      option = e ? '发布' : '禁止可发布';
    } else if (type === 'isSrvAcceptSubscribe') {
      option = e ? '订阅' : '禁止可订阅';
    }

    queryBrokerListByTopicNameQuery
      .run({
        data: {
          topicName,
          brokerId: '',
        },
      })
      .then((d: TopicResultData) => {
        onOpenModal({
          type: 'topicStateChange',
          title: `请确认操作`,
          updateFunction: updateModelParams,
          params: {
            option,
            value: e,
            topicName,
            data: d[0].topicInfo,
            type,
            callback: () => {
              if (type === 'isSrvAcceptPublish') {
                setIsSrvAcceptPublish(e);
              } else if (type === 'isSrvAcceptSubscribe') {
                setIsSrvAcceptSubscribe(e);
              }
            },
          },
        });
      });
  };
  // author
  const onAuthorizeControl = (e: boolean) => {
    const option = e ? '发布' : '禁止可发布';
    const topicName = queryTopicConf.data[0].topicInfo[0].topicName;
    onOpenModal({
      type: 'authorizeControl',
      title: `请确认操作`,
      updateFunction: updateModelParams,
      params: {
        option,
        value: e,
        topicName,
        callback: () => {
          setEnableAuthControl(e);
        },
      },
    });
  };
  // edit topic
  const onEdit = (r?: TopicResultData) => {
    const p = r || queryTopicConf.data[0].topicInfo[0];
    onOpenModal({
      type: 'editTopic',
      title: '编辑Topic',
      updateFunction: updateModelParams,
      params: {
        ...p,
        callback: (d: any) => {
          onOpenModal({
            type: 'chooseBroker',
            title: '选择【修改】broker列表',
            updateFunction: updateModelParams,
            params: {
              data: d,
              subType: 'edit',
              callback: () => {
                onOpenModal({
                  type: 'close',
                  updateFunction: updateModelParams,
                });
              },
            },
          });
        },
      },
    });
  };
  // reload topic
  const queryBrokerInfo = useRequest<any, any>(
    data => ({ url: '/api/op_query/admin_query_broker_run_status', ...data }),
    { manual: true }
  );
  const onReload = (r: TopicResultData) => {
    queryBrokerInfo
      .run({
        data: {
          brokerId: r.brokerId,
        },
      })
      .then(data => {
        onOpenBrokerModal({
          type: 'reload',
          title: `确认进行【重载】操作？`,
          updateFunction: updateBrokerModalParams,
          params: [data[0].brokerId],
        });
      });
  };
  // on delete broker
  const onDeleteBroker = (r: TopicResultData) => {
    queryBrokerListByTopicNameQuery
      .run({
        data: {
          topicName: r.topicName,
          brokerId: r.brokerId,
        },
      })
      .then((d: TopicResultData) => {
        onOpenModal({
          type: 'deleteTopic',
          title: `请确认操作`,
          updateFunction: updateModelParams,
          params: {
            topicName: r.topicName,
            data: d[0].topicInfo,
          },
        });
      });
  };
  const onDeleteConsumeGroup = (r: TopicResultData) => {
    onOpenModal({
      type: 'deleteConsumeGroup',
      title: `请确认消费组`,
      updateFunction: updateModelParams,
      params: {
        topicName: r.topicName,
        groupName: r.groupName,
      },
    });
  };

  return (
    <Spin spinning={queryTopicConf.loading && queryTopicInfo.loading}>
      <Breadcrumb
        breadcrumbMap={breadMap}
        appendParams={`Topic（${name}）详情`}
      />
      <div className="main-container">
        <TitleWrap
          title="基本信息"
          wrapperStyle={{ position: 'relative' }}
          hasSplit={false}
        >
          <div className="topic-detail-options-wrapper">
            <Switch
              className="mr10"
              checked={isSrvAcceptPublish}
              checkedChildren="订阅"
              unCheckedChildren="订阅"
              onChange={e => onSwitchChange(e, 'isSrvAcceptPublish')}
            />
            <Switch
              className="mr10"
              checked={isSrvAcceptSubscribe}
              checkedChildren="发布"
              unCheckedChildren="发布"
              onChange={e => onSwitchChange(e, 'isSrvAcceptSubscribe')}
            />
            <Switch
              className="mr10"
              checked={enableAuthControl}
              checkedChildren="权限可控"
              unCheckedChildren="权限可控"
              onChange={e => onAuthorizeControl(e)}
            />
          </div>
          <Form form={form}>
            <Row gutter={24}>
              {queryTopicConf.data &&
                Object.keys(queryTopicConf.data[0]).map(
                  (t: string, index: number) => {
                    const label = transParamsWithConstantsMap(
                      TOPIC_INFO_ZH_MAP,
                      t
                    );
                    const ignoreList = [
                      'isSrvAcceptPublish',
                      'isSrvAcceptSubscribe',
                    ];
                    if (
                      queryTopicConf.data[0][t] instanceof Object ||
                      !label ||
                      ignoreList.includes(t)
                    )
                      return null;
                    return (
                      <Col span={12} key={'queryTopicConf' + index}>
                        <Form.Item labelCol={{ span: 12 }} label={label}>
                          {queryTopicConf.data[0][t] + ''}
                        </Form.Item>
                      </Col>
                    );
                  }
                )}
            </Row>
          </Form>
        </TitleWrap>
        <TitleWrap title="缺省配置" wrapperStyle={{ position: 'relative' }}>
          <div className="topic-detail-options-wrapper">
            <Button
              className="mr10"
              type="primary"
              size="small"
              onClick={() => onEdit()}
            >
              编辑
            </Button>
          </div>
          <Form form={form}>
            <Row gutter={24}>
              {[
                'acceptPublish',
                'acceptSubscribe',
                'unflushThreshold',
                'unflushInterval',
                'deleteWhen',
                'deletePolicy',
                'numPartitions',
              ].map((t: string, index: number) => {
                if (
                  !queryTopicConf.data ||
                  !queryTopicConf.data[0].topicInfo[0]
                )
                  return null;
                const value = queryTopicConf.data[0].topicInfo[0][t];
                return (
                  <Col span={12} key={'queryTopicConf' + index}>
                    <Form.Item labelCol={{ span: 12 }} label={t}>
                      {value + ''}
                    </Form.Item>
                  </Col>
                );
              })}
            </Row>
          </Form>
        </TitleWrap>
        <TitleWrap title="部署Broker列表">{renderBrokerList()}</TitleWrap>
        <TitleWrap title="消费组列表">{renderConsumeGroupList()}</TitleWrap>
      </div>
      <CommonModal
        modalParams={modalParams}
        data={[queryTopicConf.data && queryTopicConf.data[0]]}
      />
      <BrokerModal
        modalParams={brokerModalParams}
        data={[queryBrokerInfo.data && queryBrokerInfo.data[0]]}
      />
    </Spin>
  );
};

export default Detail;
